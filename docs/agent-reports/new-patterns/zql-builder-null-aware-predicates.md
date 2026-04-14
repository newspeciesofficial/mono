# New pattern: NULL-aware predicate short-circuiting

## Category

A (Language) / D (Data) — a zql semantic rule not present in zero-cache. The
guide covers simple comparison and pattern matching but does not specify how
`NULL` values interact with `=` / `!=` / `<` / `LIKE` (they short-circuit to
`false`) versus `IS` / `IS NOT` (where `NULL` participates). The zqlite
gotcha at the bottom of `AGENTS.md` is about SQLite query-planner behaviour,
not about predicate evaluation semantics.

## Where used

- `packages/zql/src/builder/filter.ts:62-93` — `createPredicate` splits
  `IS` / `IS NOT` from all other operators. `IS` / `IS NOT` pass through to
  `createIsPredicate`; everything else uses `createPredicateImpl` wrapped in
  two null guards: if the right-hand side is null, the predicate is always
  `false`; if the left-hand row value is null at runtime, the predicate is
  `false`.
- `packages/zql/src/builder/filter.ts:96-106` — `createIsPredicate`: strict
  `===` / `!==` comparison, so `IS NULL` matches only `null` (not
  `undefined`, but `Row` values come out of `Value` which is
  `string | number | bigint | boolean | null` — `undefined` never appears
  except for a missing column key, which `createPredicate` handles with the
  same branch).
- `packages/zql/src/builder/filter.ts:108-150` — `createPredicateImpl`
  returns `SimplePredicateNoNull`, a type asserting the caller has stripped
  nulls.
- `packages/zql/src/builder/like.ts:9-13` — `getLikePredicate` calls
  `assertString(lhs)` under the same assumption: nulls were already removed
  upstream.

The guarantee this encodes is: **any comparison with a null operand returns
false, except for `IS NULL` / `IS NOT NULL` which test null explicitly.**
This matches SQL three-valued-logic truncated to "UNKNOWN → false" at the
`WHERE` boundary (rows with UNKNOWN results are filtered out).

## TS form

```ts
// IS / IS NOT: null participates, strict equality
switch (condition.op) {
  case 'IS':
  case 'IS NOT': {
    const impl = createIsPredicate(right.value, condition.op);
    if (left.type === 'literal') {
      const result = impl(left.value);
      return () => result;
    }
    return (row: Row) => impl(row[left.name]);
  }
}

// All other operators: null on either side -> false
if (right.value === null || right.value === undefined) {
  return (_row: Row) => false;
}
const impl = createPredicateImpl(right.value, condition.op);
return (row: Row) => {
  const lhs = row[left.name];
  if (lhs === null || lhs === undefined) {
    return false;
  }
  return impl(lhs);
};
```

## Proposed Rust form

```rust
use serde_json::Value;            // or a domain-specific `enum ZqlValue { Null, Bool(bool), Num(..), Str(String), BigInt(i128) }`

pub type Predicate = Box<dyn Fn(&Row) -> bool + Send + Sync>;

pub fn create_predicate(cond: &NoSubqueryCondition) -> Predicate {
    match cond {
        NoSubqueryCondition::And(cs) => {
            let ps: Vec<_> = cs.iter().map(create_predicate).collect();
            Box::new(move |row| ps.iter().all(|p| p(row)))
        }
        NoSubqueryCondition::Or(cs) => {
            let ps: Vec<_> = cs.iter().map(create_predicate).collect();
            Box::new(move |row| ps.iter().any(|p| p(row)))
        }
        NoSubqueryCondition::Simple(s) => create_simple_predicate(s),
    }
}

fn create_simple_predicate(c: &SimpleCondition) -> Predicate {
    match c.op {
        Op::Is | Op::IsNot => {
            // null-participating equality
            let rhs = c.right.literal().clone();
            let op = c.op;
            match &c.left {
                LeftSide::Literal(l) => {
                    let r = is_eq(l, &rhs, op);
                    Box::new(move |_row| r)
                }
                LeftSide::Column(name) => {
                    let name = name.clone();
                    Box::new(move |row| is_eq(row.get(&name).unwrap_or(&Value::Null), &rhs, op))
                }
            }
        }
        other => {
            let rhs = c.right.literal().clone();
            if rhs.is_null() {
                return Box::new(|_row| false);        // short-circuit: null rhs
            }
            let impl_fn = build_non_null_op(other, rhs);
            match &c.left {
                LeftSide::Literal(l) if l.is_null() => Box::new(|_row| false),
                LeftSide::Literal(l) => {
                    let r = impl_fn(l);
                    Box::new(move |_row| r)
                }
                LeftSide::Column(name) => {
                    let name = name.clone();
                    Box::new(move |row| match row.get(&name) {
                        None | Some(Value::Null) => false,
                        Some(v) => impl_fn(v),
                    })
                }
            }
        }
    }
}

fn is_eq(lhs: &Value, rhs: &Value, op: Op) -> bool {
    match op {
        Op::Is    => lhs == rhs,
        Op::IsNot => lhs != rhs,
        _ => unreachable!(),
    }
}
```

## Classification

- **Idiom-swap**. The algorithm is identical; only the null representation
  changes. In TS, nulls come through as JS `null` with an occasional
  `undefined` for missing keys; in Rust, use a single `Value::Null` (or
  `Option<Value>` at the column lookup site) and fold "missing key" into
  "null" at the `row.get(...)` call. The two-branch structure (`IS`/`IS NOT`
  vs everything-else-short-circuits) must be preserved bit-for-bit or
  behaviour diverges.

## Caveats

- `Row` in Rust must expose a lookup that distinguishes "column present with
  null value" from "column missing". The TS code collapses them
  (`row[left.name]` returns `undefined` for missing, and the null guard
  handles both). Matching behaviour: treat both as null at the predicate
  boundary.
- For `BigInt` / `i128` columns, `==` must be value-equality (not bitwise
  pointer equality). `serde_json::Value` handles this by comparing
  `Number`s via `PartialEq` on their string representation when
  `arbitrary_precision` is on — see guide risk item 3. A domain-specific
  `ZqlValue` enum avoids the issue entirely and is recommended.
- The TS code asserts `right.type !== 'static'` and
  `left.type !== 'static'` at predicate-build time (`filter.ts:53-60`).
  This is a protocol invariant enforced upstream by
  `bindStaticParameters` (`builder.ts:145-201`). Rust should enforce the
  same invariant at the type level by splitting the AST type:
  `UnboundValuePosition` (includes `static`) versus `BoundValuePosition`
  (only `literal` / `column`). Then `create_predicate` takes
  `BoundCondition` and the static-vs-bound distinction is a compile error,
  not an `assert!` at runtime.

## Citation

- PostgreSQL null-handling in `WHERE`:
  https://www.postgresql.org/docs/current/functions-comparison.html
  ("Ordinary comparison operators yield null (signifying 'unknown'), not
  true or false, when either input is null. … Do not write `expression = NULL`
  because NULL is not equal to NULL.")
- SQL three-valued logic and `WHERE` truncation to false:
  https://www.postgresql.org/docs/current/functions-logical.html
- `serde_json::Value::is_null`:
  https://docs.rs/serde_json/latest/serde_json/enum.Value.html#method.is_null
