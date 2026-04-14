# New pattern: SQL LIKE / ILIKE pattern matching

## Category

A (Language) — standard-library usage that differs between TS and Rust; the
zero-cache translation guide does not catalogue regex construction because
zero-cache proper doesn't use it. `zql/src/builder/like.ts` translates SQL
`LIKE` / `ILIKE` / `NOT LIKE` / `NOT ILIKE` patterns into predicates by
compiling them to JS `RegExp`. Rust needs a mechanical equivalent.

## Where used

- `packages/zql/src/builder/like.ts:4-13` — `getLikePredicate(pattern, flags)`
- `packages/zql/src/builder/like.ts:15-31` — `getLikeOp` short-circuits the
  no-wildcard case to plain string equality (the common fast path).
- `packages/zql/src/builder/like.ts:33-71` — `patternToRegExp` walks the SQL
  pattern and emits a JS regex, handling `%` → `.*`, `_` → `.`, backslash
  escapes, and re-escaping of regex metacharacters.
- Consumed by `packages/zql/src/builder/filter.ts:125-132` for the
  `LIKE`/`NOT LIKE`/`ILIKE`/`NOT ILIKE` cases of `createPredicateImpl`.

## TS form

```ts
export function getLikePredicate(
  pattern: NonNullValue,
  flags: 'i' | '',
): SimplePredicateNoNull {
  const op = getLikeOp(String(pattern), flags);
  return (lhs: NonNullValue) => {
    assertString(lhs);
    return op(String(lhs));
  };
}

function getLikeOp(pattern: string, flags: 'i' | ''): (lhs: string) => boolean {
  if (!/_|%|\\/.test(pattern)) {
    if (flags === 'i') {
      const rhsLower = pattern.toLowerCase();
      return (lhs: string) => lhs.toLowerCase() === rhsLower;
    }
    return (lhs: string) => lhs === pattern;
  }
  const re = patternToRegExp(pattern, flags);
  return (lhs: string) => re.test(lhs);
}

// patternToRegExp emits '^' + translated_body + '$' with the 'm' flag and
// optionally 'i'; escapes JS regex special chars in literal segments.
```

## Proposed Rust form

```rust
use regex::Regex;

pub enum LikePredicate {
    EqSensitive(String),
    EqInsensitive(String),           // both sides lowercased
    Regex(Regex),
}

impl LikePredicate {
    pub fn compile(pattern: &str, case_insensitive: bool) -> Result<Self, regex::Error> {
        if !pattern.contains(['_', '%', '\\']) {
            return Ok(if case_insensitive {
                LikePredicate::EqInsensitive(pattern.to_lowercase())
            } else {
                LikePredicate::EqSensitive(pattern.to_string())
            });
        }
        let mut out = String::from("^");
        let mut chars = pattern.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '%' => out.push_str(".*"),
                '_' => out.push('.'),
                '\\' => {
                    let next = chars.next().ok_or_else(||
                        regex::Error::Syntax("LIKE pattern must not end with escape character".into()))?;
                    escape_and_push(&mut out, next);
                }
                other => escape_and_push(&mut out, other),
            }
        }
        out.push('$');
        let mut b = regex::RegexBuilder::new(&out);
        b.multi_line(true).case_insensitive(case_insensitive);
        Ok(LikePredicate::Regex(b.build()?))
    }

    pub fn matches(&self, lhs: &str) -> bool {
        match self {
            LikePredicate::EqSensitive(p) => lhs == p,
            LikePredicate::EqInsensitive(p) => lhs.to_lowercase() == *p,
            LikePredicate::Regex(re) => re.is_match(lhs),
        }
    }
}

fn escape_and_push(out: &mut String, c: char) {
    // Mirror the TS specialCharsRe set: $()*+.?[]\^{|}
    if matches!(c, '$' | '(' | ')' | '*' | '+' | '.' | '?'
                 | '[' | ']' | '\\' | '^' | '{' | '|' | '}') {
        out.push('\\');
    }
    out.push(c);
}
```

## Classification

- **Idiom-swap**. Same structural algorithm; swap `RegExp` for `regex::Regex`
  and JS char iteration for `char_indices`. Behaviour-for-behaviour match
  requires the `multi_line(true)` flag (TS uses flag `'m'`) and identical
  escape semantics. Compile errors become `Result` instead of thrown
  exceptions (A15 + F1 in the guide).

## Caveats

- JS `RegExp` with flag `'m'` causes `^` / `$` to match per-line. Rust's
  `regex` crate needs `.multi_line(true)` to match that. Note that the TS
  code unconditionally appends `'m'` — this matters if input `lhs` contains
  newlines.
- The `regex` crate rejects a few patterns the JS engine accepts (e.g.
  look-around). The SQL LIKE translation only emits `.`, `.*`, and escaped
  literals, so this is not a hazard here. If the input pattern itself
  somehow contained raw regex metacharacters they'd already be escaped by
  `escape_and_push`, so no surprises.
- `regex::Regex::new` does linear-time DFA/NFA matching and does not back-
  track, giving us ReDoS immunity the JS version doesn't have.
- Precompile per `(pattern, flags)` tuple once at predicate-creation time,
  never per row — matches the TS hot-path optimisation at
  `like.ts:15-28`.

## Citation

- `regex` crate, "Syntax" section on case-insensitive and multi-line flags:
  https://docs.rs/regex/latest/regex/#grouping-and-flags
- `regex::RegexBuilder::multi_line` and `case_insensitive`:
  https://docs.rs/regex/latest/regex/struct.RegexBuilder.html
- SQL `LIKE` specification (`%` and `_`, escape char default `\`):
  https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-LIKE
