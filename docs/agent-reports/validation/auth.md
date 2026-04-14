# Validation report: auth

## Scope
- `packages/zero-cache/src/auth/auth.ts` — 280 LOC
- `packages/zero-cache/src/auth/jwt.ts` — 89 LOC
- `packages/zero-cache/src/auth/read-authorizer.ts` — 152 LOC
- `packages/zero-cache/src/auth/write-authorizer.ts` — 594 LOC
- `packages/zero-cache/src/auth/load-permissions.ts` — 100 LOC
- Total LOC: 1215

Transitive modules inspected only to confirm that symbols imported here map
to existing rows of `ts-vs-rs-comparison.md` (`zero-protocol/src/ast.ts`,
`zero-schema/src/compiled-permissions.ts`,
`zql/src/builder/builder.ts` — `bindStaticParameters` / `buildPipeline`,
`zql/src/ivm/stream.ts` — `consume`,
`zql/src/query/static-query.ts` — `newStaticQuery`,
`zqlite/src/table-source.ts`,
`zqlite/src/internal/sql.ts` — `sql`/`compile`,
`zero-cache/src/db/statements.ts` — `StatementRunner`,
`zero-cache/src/config/zero-config.ts`).

## Summary
| metric | count |
|---|---|
| TS constructs used | 23 |
| Constructs covered by guide | 22 |
| Constructs NOT covered | 1 |
| Libraries imported | 4 |
| Libraries covered by guide | 4 |
| Libraries NOT covered | 0 |
| Risk-flagged items the module depends on | 4 |

## Constructs used

All file:line samples below are given with a filename prefix.

| Construct (TS) | Where (file:line sample) | Guide section | Status |
|---|---|---|---|
| `class AuthSessionImpl implements AuthSession` with `readonly id`, `#auth`, `#boundUserID`, `#revision`, and getters `get auth()`/`get revision()` | auth.ts:70, 88, 92 | A1, A5, A23 | covered |
| `interface AuthSession { update(...)/revision/auth/clear() }` | auth.ts:25 | A5 | covered |
| Tagged-union type `Auth = OpaqueAuth \| JWTAuth` with `readonly type: 'jwt' \| 'opaque'` and `authEquals()` switch on `type`/`raw` | auth.ts:12, 18, 23, 60 | A6 | covered |
| String-literal union `Phase = 'preMutation' \| 'postMutation'` + `switch(phase)` gating of `rowPolicies.insert / update.preMutation / update.postMutation / delete` | write-authorizer.ts:54, 386, 415 | A6 | covered |
| Result type `AuthUpdateResult = { ok: true } \| { ok: false; error: ErrorBody }` | auth.ts:42 | A6 (discriminated union), F1 | covered |
| Generic helper `#timedCanDo<A extends keyof ActionOpMap>(...)` with associated-op-type table `type ActionOpMap = { insert: InsertOp; update: UpdateOp; delete: DeleteOp }` | write-authorizer.ts:333, 590 | A8, A9 (associated type) | covered |
| `class WriteAuthorizerImpl implements WriteAuthorizer` with many `#private` fields, `Map<string, TableSource>` cache, `BuilderDelegate` built from arrow-function methods | write-authorizer.ts:75, 105 | A1, A5, A23 | covered |
| Async `canPreMutation` / `canPostMutation` sequencing `await`-in-for-loop evaluation with early-return on first `false` | write-authorizer.ts:134, 158 | A17 (sequential await), C9 (cancel not needed here; no race) | covered |
| `this.#statementRunner.beginConcurrent()` + `try { … } finally { this.#statementRunner.rollback() }` to sandbox post-mutation state on the replica | write-authorizer.ts:162, 220 | **Part 3 Risk #2** (BEGIN CONCURRENT) | covered (risk-flagged) |
| IVM mutation simulation: `source.push({type:'add'/'edit'/'remove', row, oldRow?})` + `consume(...)` to feed a TableSource inside the sandboxed txn | write-authorizer.ts:168, 182, 192 | crate table: `zql/src/ivm` (row 14 of comparison) | covered (risk-flagged) |
| `buildPipeline(rowQueryAst, builderDelegate, 'query-id')` + `input.fetch({}); for (const _ of res) return true; input.destroy();` | write-authorizer.ts:555–565 | crate table: `zql` row (comparison row 14 — Stub) | covered (risk-flagged) |
| `newStaticQuery(schema, tableName).where(pk, '=', val)` fluent builder on AST | write-authorizer.ts:377, 382 | covered by row 1/14 of comparison | covered |
| Recursive AST traversal in `transformCondition` with `switch(cond.type)` over `'simple' \| 'and' \| 'or' \| 'correlatedSubquery'` | read-authorizer.ts:127–151 | A6 | covered |
| AST spread-update `{...query, where: simplifyCondition(...), related: query.related?.map(...)}` | read-authorizer.ts:92–102 | A13, A20 | covered |
| `bindStaticParameters(ast, {authData, preMutationRow?})` | read-authorizer.ts:56, write-authorizer.ts:540 | comparison row 2 (**Port partial**) | covered (risk-flagged) |
| Exception hierarchy: `throw new ProtocolError({...})` + `isProtocolError(e)` discriminator; plain `Error` with `{cause: e}` | auth.ts:211, 229, 274, load-permissions.ts:53 | A15, F1, F2, F3 | covered |
| Optional chaining + `!= undefined` guards: `this.#auth?.type`, `rules?.row`, `policy.update?.preMutation` | auth.ts:157, 207, write-authorizer.ts:376, 393, 422 | A11, A21 | covered |
| Nullish coalescing / assignment: `this.#boundUserID ?? 'unknown'`, `this.#boundUserID ??= userID` | auth.ts:99, 170 | A12 | covered |
| `valita` runtime schema: `v.parse(obj, permissionsConfigSchema)`, `v.parse(primaryKeyValues[pk], primaryKeyValueSchema)` | load-permissions.ts:50, write-authorizer.ts:475 | E1/E2 (serde custom Deserialize) | covered |
| `JSON.parse` on a DB-returned permissions blob | load-permissions.ts:49 | E1/E2 | covered |
| `TextEncoder().encode(secret)` to turn a shared secret into `Uint8Array` for HMAC JWT | jwt.ts:78 | B3 (Uint8Array → `Vec<u8>` / `&[u8]`) | covered |
| Module-level mutable singleton `let remoteKeyset: ReturnType<typeof createRemoteJWKSet> \| undefined` with lazy init in `getRemoteKeyset(jwksUrl)` | jwt.ts:31–38 | Part 2 crate row for `jose` (→ `jsonwebtoken` + `jwks-client-rs` / `jwtk`) | covered |
| `performance.now()` in `#timedCanDo` plus `try/finally` log of action duration | write-authorizer.ts:339, 348 | B7 | covered |
| `must(x)` / `assert(cond, () => msg)` for invariant checks | write-authorizer.ts:311, 373, 498, 573 | A6 | covered |
| `Object.entries` / `Object.fromEntries` for cellPolicies loop and schema construction | write-authorizer.ts:409, load-permissions.ts:87 | A19 | covered |
| Hashing query ASTs for cache keys: `hashOfAST(transformed)` | read-authorizer.ts:38 | covered by row 1 of comparison (types/ast) | covered |
| `MaybePromise<T>` return type to avoid awaiting when policy is empty (`#passesPolicy` returns `false` synchronously when `policy === undefined \|\| policy.length === 0`, otherwise a Promise-returning branch) | write-authorizer.ts:532, 533, 536, 560 | — | **NEW — see `new-patterns/auth-maybe-promise-sync-fast-path.md`** |

## Libraries used

| TS package | Rust crate (from guide Part 2) | Status |
|---|---|---|
| `jose` (`jwtVerify`, `createRemoteJWKSet`, `generateKeyPair`, `exportJWK`, `JWTPayload`, `JWTClaimVerificationOptions`, `JWK`, `KeyLike`) | `jsonwebtoken` + `jwks-client-rs` (or `jwtk` / `josekit` for JWKS + JWK export combined) | covered (Part 3 Risk #? — see below) |
| `@rocicorp/logger` (`LogContext.withContext`, `.debug`/`.info`/`.warn`) | `tracing` + `tracing-subscriber` | covered |
| `@databases/sql` (`SQLQuery` type used only as a type for the compiled sql fragment list in write-authorizer) | `postgres-protocol::escape` helpers + local `sql!` macro, or inline string formatting with `sql::ident` from `zqlite/internal/sql.ts` | covered (the SQL builder itself is internal `zqlite/src/internal/sql.ts`; only the `SQLQuery` type name is imported from `@databases/sql`) |
| `@opentelemetry/resources` (`MaybePromise` type alias only — unusual import path; no runtime dep) | `tracing` + `tracing-opentelemetry` (type alias itself doesn't translate — see new-pattern) | covered (as type only) |

No third-party package imported in these five files is outside Part 2 of the
guide. `shared/*`, `zero-protocol/*`, `zero-schema/*`, `zero-types/*`,
`zql/*`, `zqlite/*`, and in-package siblings are internal modules covered by
other rows of `ts-vs-rs-comparison.md` (rows 1, 2, 14, and the zqlite rows).

## Risk-flagged items depended on

1. **`bindStaticParameters` (comparison row 2 — Port partial).** Both read and
   write authorizers rely on it:
   - `read-authorizer.ts:56` binds `authData` into the permissioned AST before
     execution.
   - `write-authorizer.ts:540` does the same for each policy check and also
     passes a `preMutationRow` anchor.

   Comparison row 2 notes that the Rust port wires `authData.sub` but has
   not been tested against `preMutationRow`. Until that is closed, cell-level
   and row-level policies that key on `preMutationRow.*` (a common pattern)
   will mis-evaluate.

2. **IVM pipeline-driver (comparison row 14 — Stub).** Write-auth depends on
   the full IVM pipeline to evaluate policies: it builds a `rowQuery` via
   `newStaticQuery(schema, table).where(pk, '=', val)`, rewrites the `where`
   via `updateWhere(ast.where, policy)` (an `AND (where OR any-rule-in-policy)`
   combinator), and then runs `buildPipeline(ast, delegate).fetch({})`
   against an in-memory `TableSource` backed by the replica. If any single
   row is returned the policy passes. Without a real operator graph,
   policies that use `whereExists` / correlated subqueries / OR-of-ANDs will
   be mis-evaluated.

3. **`BEGIN CONCURRENT` on SQLite (Part 3 Risk #2).** `canPostMutation`
   depends on running mutations against a sandboxed snapshot of the replica:
   `this.#statementRunner.beginConcurrent()` followed by `source.push({type:
   'add'/'edit'/'remove'})` into a `TableSource` and a final `rollback()`
   in `finally` (write-authorizer.ts:162, 220). Without BEGIN CONCURRENT the
   replica sees actual writes from auth checks, which would poison every
   subsequent read-transaction. The Rust port must either build libsqlite3
   with the begin-concurrent branch or gate write-auth behind a strictly
   read-only simulation layer.

4. **JWT signature verification (comparison row 20 — Missing).** `jwt.ts`
   currently supports three key-source modes (inline JWK, inline shared
   secret, remote JWKS URL). The Rust port has none of them implemented;
   only payload decoding (without signature verification) is currently
   done. The `pickToken` path in `auth.ts` also relies on a valid
   `decoded.sub` and `decoded.iat` coming from a verified payload to make
   token-rotation decisions — an unverified token would let any attacker
   stick a higher `iat` and win rotation every time.

Additional notable semantics (already covered but worth flagging):

- `pickToken` (auth.ts:197–280) enforces that a client group's token type
  cannot change (opaque ↔ JWT) and that `sub` cannot change across
  rotations. The Rust port must preserve those invariants literally
  including the `ProtocolError` payload with
  `{kind: Unauthorized, origin: ZeroCache}`.
- `read-authorizer.ts:68` defaults to an **empty `or`** (i.e. "allow
  nothing") when no `select` rules are defined for a table. Missing rules
  are a closed-world deny. The Rust port must not silently fall through.
- `write-authorizer.ts:376, 378` reads `rules?.row`, not `rules?.row?.select`;
  write rules live under `row.insert / row.update.preMutation /
  row.update.postMutation / row.delete`. Cell rules live under
  `rules?.cell?.[column].{insert|update.preMutation|update.postMutation|delete}`.
  The evaluation order is documented inline (write-authorizer.ts:357–366):
  every applicable policy must pass; a single failing policy denies the op.
- `validateTableNames` (write-authorizer.ts:250) and `normalizeOps` (:227)
  are independent entry points — the Rust port must expose them as separate
  functions on the trait.
- `updateWhere` (write-authorizer.ts:572) asserts the pre-existing `where`
  is non-null (the pk equality clauses built by `newStaticQuery(...)
  .where(pk, '=', val)`) and composes it with an `OR` of every rule's
  condition.

## Ready-to-port assessment

**Blocked on four prior rows but otherwise mechanically portable.** Every
language construct in these five files maps onto an existing guide section
(A1, A5, A6, A8, A11, A12, A13, A15, A17, A19, A20, A21, A22, A23; B3, B7;
E1/E2; F1/F2/F3). Every third-party crate has a Part 2 row.

One genuinely new idiom came out of this scope and needs a guide entry
before translation:

1. **`auth-maybe-promise-sync-fast-path.md`** — `#passesPolicy` declares
   `: MaybePromise<boolean>` and returns a synchronous `false` when the
   policy is empty, avoiding an `await` frame. In Rust the equivalent is
   two options: (a) keep the function `async fn` and let the compiler
   inline the trivial branch — costs one zero-work `poll`, (b) make the
   function return `impl Future<Output=bool>` and branch to
   `futures::future::ready(false)`. The guide currently has no entry for
   "sync-vs-async union return type", so the translator will guess.

Blockers per the dependency graph (all already tracked in
`ts-vs-rs-comparison.md`):

- **Row 2** — `bindStaticParameters` needs `preMutationRow` plumbed. Read
  and write authorizers both depend on this; write-auth is unusable until
  it lands.
- **Row 14** — IVM pipeline-driver. `buildPipeline(ast, delegate).fetch({})`
  inside `#passesPolicy` is the entire enforcement mechanism; without an
  operator graph it can only evaluate flat WHEREs.
- **Row 20** — JWT signature verification + write authorizer. Both this
  module's top-level `verifyToken` and the full `WriteAuthorizerImpl` are
  marked Missing; the TS source in this scope is the reference.
- **Part 3 Risk #2** — `BEGIN CONCURRENT`. `canPostMutation` cannot be
  ported safely to stock `rusqlite`.

If all four items above are closed, everything else here (including the
`jose` → `jsonwebtoken` + `jwks-client-rs` swap, the `TextEncoder`→
`&[u8]` swap for shared-secret verification, the module-level singleton
for the remote JWKS, the `MaybePromise` return type, and the
read-authorizer's AST rewrite) is a mechanical port.
