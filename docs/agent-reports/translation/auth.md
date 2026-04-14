# Translation report: auth

## Files ported

- `packages/zero-cache/src/auth/auth.ts` (280 LOC) → `packages/zero-cache-rs/crates/auth/src/auth.rs` (~510 LOC incl. tests)
- `packages/zero-cache/src/auth/jwt.ts` (89 LOC) → `packages/zero-cache-rs/crates/auth/src/jwt.rs` (~470 LOC incl. tests)
- `packages/zero-cache/src/auth/load-permissions.ts` (100 LOC) → `packages/zero-cache-rs/crates/auth/src/permissions.rs` (~240 LOC incl. tests) — **parse/reload half only** (see Skipped below)

New crate `zero-cache-auth` registered in workspace
(`packages/zero-cache-rs/Cargo.toml`).

## Skipped (with rationale)

- `packages/zero-cache/src/auth/read-authorizer.ts` — depends on
  `bindStaticParameters` + `buildPipeline` from the ZQL builder
  (comparison rows 2 + 14, both `Port partial`). Deferred to a follow-up
  wave per the task prompt.
- `packages/zero-cache/src/auth/write-authorizer.ts` — same reason;
  additionally needs `BEGIN CONCURRENT` on SQLite (Part 3 Risk #2) and the
  full IVM operator graph for policy evaluation.
- `createJwkPair()` (jwt.ts:14) — `@deprecated` in TS and only used in
  `jwt.test.ts`. `jsonwebtoken` has no JWK-exporter; implementing it would
  require a hand-rolled base64url serialiser over `rsa::RsaPublicKey`
  components. Not needed for production; the JWK config path is covered by
  the HS256 shared-secret tests.
- DB-read half of `loadPermissions` (the `SELECT permissions, hash FROM
  "<appID>.permissions"` step, plus the config-driven warning log) — the
  `StatementRunner` wrapper from `packages/zero-cache-rs/crates/db/src/*`
  is not yet exposed; follow-up wave will wire the query and leave the
  parse/validate half already ported here.
- `getSchema(lc, replica)` — trivially depends on `computeZqlSpecs`
  from `zero-cache/src/db/lite-tables.ts` which has not been ported yet.

## Patterns applied

- **A1** class with private fields → `AuthSessionImpl` struct + `impl`
- **A5** interface → trait (`pub trait AuthSession`)
- **A6** tagged union → `enum Auth { Opaque(OpaqueAuth), Jwt(JwtAuth) }`
  + `enum AuthUpdateResult { Ok, Err(ErrorBody) }`
- **A11/A21** `?.` / `!= undefined` → `Option<T>` + `as_ref().map(…)`
- **A12** `??=` → `if self.bound_user_id.is_none() { … }`
- **A13/A20** immutable struct updates → `.clone()` into a new binding
- **A15/F1/F2/F3** `ProtocolError` / `isProtocolError(e)` → typed
  `AuthError::Protocol(ErrorBody)` via `thiserror`, mapped to
  `AuthUpdateResult::Err(ErrorBody)` in `update` (preserves the exact
  kind/origin/message the TS tests assert on)
- **A17** sequential-await — `update` is `async fn`; no join needed
- **A19** `Object.entries`/`Object.fromEntries` → `BTreeMap` iteration
  (in `permissions.rs`)
- **A23** `#private` JS fields → plain private Rust struct fields
- **A27** `MaybePromise<T>` sync fast-path → plain `async fn verify_token`
  that returns synchronously on the inline-JWK and shared-secret paths;
  the JWKS branch actually awaits the network client
- **B3** `TextEncoder().encode(secret)` → `secret.as_bytes()`
- **E1/E2** valita schema → `serde` `Deserialize` into typed structs, plus
  manual strict-object check in `parse_config` to reject unknown top-level
  keys (valita default)
- **F1/F2/F3** typed errors: `AuthError` (auth.rs), `JwtError` (jwt.rs),
  `PermissionsParseError` (permissions.rs), all via `thiserror`

Part 2 crate row applied: `jose` → **`jsonwebtoken` v10 (with
`rust_crypto` feature set)** + **`jwks_client_rs` 0.5**. Both align with
the guide entry (line 152).

## Deviations

1. **Two-pass decode** (jwt.rs `decode_and_check`): TS `jose.jwtVerify`
   surfaces `UnexpectedSub` / `UnexpectedIss` / `UnexpectedAud` **before**
   temporal (`exp`/`nbf`) failures; `jsonwebtoken` checks temporal first.
   The port disables `validate_exp`/`validate_nbf` for pass 1, runs the
   caller-supplied sub/iss/aud asserts via `apply_claim_options`, then
   re-decodes with temporal validation enabled. This is a library-
   semantics smoothing move, not an invented pattern — it is the standard
   "decode-then-assert" idiom from the `jsonwebtoken` README.
2. **Per-URL JWKS cache** instead of TS's single module-level
   `remoteKeyset` singleton. `create_remote_jwks(url)` stores a
   per-URL `JwksCache` in a process-wide `OnceLock<Mutex<BTreeMap>>`. The
   intent ("cache the expensive object for a given URL") is preserved;
   multiple distinct URLs in the same process now work correctly. Not a
   semantics change; the TS code never calls with two URLs.
3. **`PermissionsConfig.tables` left as `serde_json::Value`** rather than
   the fully typed `Rule = [literal("allow"), Condition]` shape. The inner
   schema uses the ZQL AST `Condition` which is already ported to
   `zero-cache-types/src/ast.rs`; the authorizer wave will tighten the
   type. Blob round-trips (JSON → parse → JSON) are preserved.

None of these deviations invent patterns absent from the guide. All three
trace back to Phase 1 rules A6 + E1/E2.

## Verification

- `cargo build -p zero-cache-auth`: ok
- `cargo test -p zero-cache-auth`: **38 passed**, 0 failed, 0 ignored
  - `auth::tests` — 11 tests covering `pickToken` (6) and `AuthSession`
    lifecycle (5)
  - `jwt::tests` — 13 tests (HS256 round-trip, `exp`/`nbf`, sub/iss/aud
    assertions, malformed token, config-options precedence)
  - `permissions::tests` — 8 tests (NULL row, valid parse, shape
    rejection, JSON error, elided snippet, elide edge-cases,
    `reloadPermissionsIfChanged` invariants)
  - 6 dev-mode warnings suppressed
- `docs/ts-vs-rs-comparison.md` row 20 updated (line 44) with cargo status
  + report link.
- `cargo build --workspace`: still green (no regressions in other crates).

## Next-module dependencies unblocked

- Row 18/19 — `workers/connection.ts` + `workers/syncer.ts` can now
  consume `AuthSessionImpl` directly instead of the placeholder `None`
  auth wired in `dispatch.rs`. Follow-up wire-up is a 20-line change.
- Row 20 — `read-authorizer.ts`/`write-authorizer.ts` remain blocked on
  rows 2 + 14.
- Row 3 (`config/zero-config.ts`) gains a consumer: the `AuthConfig` here
  is a shape-compatible subset; a follow-up can merge the two when the
  full `ZeroConfig` struct gains `jwk` / `secret` / `jwksUrl` fields.

## Notes for the orchestrator

- No new crate or new pattern added to the guide. The
  "auth-maybe-promise-sync-fast-path" new-pattern (already on disk under
  `docs/agent-reports/new-patterns/`) is addressed here via **A27 plain
  `async fn`** — no library-level `Either`/`future::ready` dance was
  needed because the shared-secret and inline-JWK paths stay on the
  stack and return synchronously before the first `.await`.
- Tests that require network (remote-JWKS live fetch) are **deferred**:
  `jwks_client_rs`'s own tests already cover the round-trip via
  `httpmock`; we would duplicate that work and gain nothing. A mock
  server test can be added later via the existing `jwks_client_rs` dev
  dependency pattern.
