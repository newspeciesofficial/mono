# New pattern: opaque "branded" numeric type (`TTLClock`)

## Category
A (Language). Similar in spirit to A24 (newtype), but implemented via a
structural-type-system trick rather than a struct wrapper.

## Where used
- `packages/zero-cache/src/services/view-syncer/ttl-clock.ts:1-16` — the
  whole file: a phantom unique-symbol tag makes `TTLClock` assignable
  from `number` only via `ttlClockFromNumber` and convertible back only
  via `ttlClockAsNumber`.
- `packages/zero-cache/src/services/view-syncer/cvr.ts:992-1058` —
  `getInactiveQueries` computes `ttlClockAsNumber(inactivatedAt) + ttl`
  to order evictions.
- `packages/zero-cache/src/services/view-syncer/cvr-store.ts:56-97,
  486-514` — persisted column `"ttlClock" DOUBLE PRECISION`; round-trips
  via `ttlClockFromNumber(... / 1000)` to bridge the deprecated
  seconds-based columns.
- `packages/zero-cache/src/services/view-syncer/schema/cvr.ts:35` —
  `ttlClock: TTLClock` inside `InstancesRow`, backed by a
  `DOUBLE PRECISION` column.

## TS form
```ts
declare const ttlClockTag: unique symbol;

/** Opaque millisecond counter, monotonically increased by the ViewSyncer
 *  while the client group is connected; persisted across disconnects. */
export type TTLClock = {[ttlClockTag]: true};

export function ttlClockAsNumber(t: TTLClock): number {
  return t as unknown as number;
}
export function ttlClockFromNumber(n: number): TTLClock {
  return n as unknown as TTLClock;
}
```

The branded type is enforced only by TypeScript; at runtime a `TTLClock`
is just a `number`. The goal is to prevent silently mixing wall-clock
times (`lastActive`) with the TTL-clock (`ttlClock`) — they are both
numbers but mean different things.

## Proposed Rust form

Rust's newtype pattern covers this cleanly without the `unsafe as`
casts TS needs:

```rust
/// Monotonic millisecond counter, advanced only while the client group
/// is connected. Persisted on `instances.ttlClock` as `DOUBLE PRECISION`.
#[derive(
    Copy, Clone, Debug, Default, PartialEq, PartialOrd,
    serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct TtlClock(f64);

impl TtlClock {
    pub const fn new(ms: f64) -> Self { Self(ms) }
    pub fn as_f64(self) -> f64 { self.0 }
}

impl From<f64> for TtlClock { fn from(v: f64) -> Self { Self(v) } }

// If the domain needs integer ms, use u64 instead of f64. The current
// TS uses `number`, so f64 matches the wire/storage type exactly.

// postgres binding (tokio_postgres::types::FromSql / ToSql):
//   DOUBLE PRECISION <-> f64 is built-in; `#[derive(FromSql, ToSql)]`
//   on a `#[postgres(transparent)]` newtype forwards the impls.
#[derive(postgres_types::FromSql, postgres_types::ToSql)]
#[postgres(transparent)]
pub struct TtlClock(f64);
```

Arithmetic stays explicit (`ttl_clock.as_f64() + ttl_ms`), matching the
TS side's `ttlClockAsNumber(...) + ttl` idiom.

## Classification
Direct. Guide rule A24 covers newtypes for primitives; add a bullet that
TS "branded" types (with `unique symbol` phantom tags) translate to
`#[serde(transparent)]` + `#[postgres(transparent)]` newtypes in Rust.

## Caveats
- The TS storage type is `number` (IEEE-754 f64). If the Rust
  implementation prefers `u64` milliseconds, decide carefully: the
  Postgres column is `DOUBLE PRECISION`, so crossing the boundary
  loses precision above 2^53. Keeping `f64` matches the TS contract.
- `TtlClock` is independent of wall-clock time. It is written through
  `CVRStore.updateTTLClock(ttlClock, lastActive)` (cvr-store.ts:488),
  which is called from the view-syncer when it advances the clock by the
  elapsed time since the last lock entry. Don't conflate it with
  `chrono::Utc::now()` — the point of the brand is to stop that exact
  mistake.
- On first CVR creation the clock is initialised to 0
  (`ttlClockFromNumber(0)` at cvr-store.ts:268, 331). `Default::default()`
  for the `TtlClock` newtype must therefore yield `TtlClock(0.0)`.

## Citation
- TypeScript nominal types via `unique symbol` / branding pattern:
  https://github.com/microsoft/TypeScript/pull/33038
- `postgres-types` `#[postgres(transparent)]`:
  https://docs.rs/postgres-types/latest/postgres_types/derive.ToSql.html
- `#[serde(transparent)]`:
  https://serde.rs/container-attrs.html#transparent
