# New pattern: `MaybePromise<T>` return type for sync fast-path inside an async-shaped API

## Category
A (Language).

## Where used
- `packages/zero-cache/src/auth/write-authorizer.ts:528–569` — `#passesPolicy` returns `MaybePromise<boolean>`; the early-exit branches (`policy === undefined`, `policy.length === 0`) return `false` synchronously while the general branch returns a `Promise<boolean>`.

## TS form
```ts
import type {MaybePromise} from '@opentelemetry/resources';

#passesPolicy(
  policy: Policy | undefined,
  authData: JWTPayload | undefined,
  rowQuery: Query<string, Schema>,
): MaybePromise<boolean> {
  if (policy === undefined) return false;           // sync
  if (policy.length === 0)  return false;           // sync
  // ... build AST, bindStaticParameters, buildPipeline ...
  const input = buildPipeline(rowQueryAst, this.#builderDelegate, 'query-id');
  try {
    const res = input.fetch({});
    for (const _ of res) { return true; }           // still sync (iterator)
  } finally {
    input.destroy();
  }
  return false;
}
```

The value of the pattern: the caller (`#passesPolicyGroup`) does
`await this.#passesPolicy(...)` unconditionally, but for empty-policy
tables the runtime never creates a microtask — a measurable saving when
every mutation goes through several policy checks.

## Proposed Rust form
Two idiomatic options:

### Option 1 — plain `async fn`, let the compiler inline trivial branches
```rust
async fn passes_policy(
    &self,
    policy: Option<&Policy>,
    auth_data: Option<&JwtPayload>,
    row_query: &Query,
) -> bool {
    let Some(policy) = policy else { return false; };
    if policy.is_empty() { return false; }
    // ... async work ...
    true
}
```
Cost: one extra `poll` that returns `Poll::Ready(false)` immediately for
the empty branches. In practice this is below measurement noise, and the
code reads linearly.

### Option 2 — explicit `impl Future<Output = bool>` return with a ready branch
```rust
use futures::future::{self, Either, FutureExt};

fn passes_policy<'a>(
    &'a self,
    policy: Option<&'a Policy>,
    auth_data: Option<&'a JwtPayload>,
    row_query: &'a Query,
) -> impl std::future::Future<Output = bool> + 'a {
    match policy {
        None => Either::Left(future::ready(false)),
        Some(p) if p.is_empty() => Either::Left(future::ready(false)),
        Some(p) => Either::Right(async move {
            // ... real async path using p ...
            true
        }),
    }
}
```
`future::ready(false)` is a zero-cost ready-future; `Either` lets the two
arms have different concrete future types.

### Option 3 — split the API
```rust
fn passes_policy_fast(&self, policy: Option<&Policy>) -> Option<bool> {
    match policy { None => Some(false), Some(p) if p.is_empty() => Some(false), _ => None }
}

async fn passes_policy_slow(&self, policy: &Policy, ...) -> bool { /* real work */ }
```
Call sites: `if let Some(v) = fast(policy) { return v; } slow(...).await`.
This is the most honest shape and avoids any runtime cost.

Pick option 1 for the mechanical port; option 2 if a benchmark shows the
extra poll matters.

## Classification
Idiom-swap. TS leans on the fact that `await x` on a non-Promise value is
a no-op; Rust cannot do that (`.await` requires a `Future`). All three
options are mechanical.

## Caveats
- Do not reach for `async_trait::async_trait` just to preserve the exact
  TS shape — `MaybePromise` is a return type, not a trait method
  signature, so standard `async fn` on the `impl` block is enough.
- The outer `#passesPolicyGroup` in TS awaits in a loop:
  `for (const p of applicableCellPolicies) { if (!(await this.#passesPolicy(p, ...))) return false; }`.
  Under option 1 this is a straight `for … in … { if !self.passes_policy(p, ...).await { return false; } }`
  — sequential evaluation with early exit, matching the TS semantics.
- `@opentelemetry/resources` is an **unusual** place to import
  `MaybePromise` from (likely a historical accident). The Rust port has
  no such type alias; delete the import and use `impl Future` / `async fn`
  directly.

## Citation
- `futures::future::ready`:
  https://docs.rs/futures/latest/futures/future/fn.ready.html
- `futures::future::Either`:
  https://docs.rs/futures/latest/futures/future/enum.Either.html
- Rust RFC 2394 (async fn) and RFC 3185 (async fn in traits):
  https://rust-lang.github.io/rfcs/2394-async_await.html
  https://rust-lang.github.io/rfcs/3185-static-async-fn-in-trait.html
