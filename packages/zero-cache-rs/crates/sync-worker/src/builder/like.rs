//! Port of `packages/zql/src/builder/like.ts`.
//!
//! Public exports ported:
//!
//! - [`get_like_predicate`] — returns a boxed `Fn(&str) -> bool`
//!   implementing SQL `LIKE` / `ILIKE` semantics.
//! - [`LikeFlags`] — simple `i` / `''` selector port of the TS `'i' | ''`
//!   string literal union.
//!
//! ## Implementation note
//!
//! TS compiles the LIKE pattern to a `RegExp`. Rust's workspace does not
//! expose the `regex` crate to `sync-worker` (we may NOT add a new
//! dependency per the Layer 9 rules). Instead we implement a small
//! char-by-char matcher that mirrors the TS regex:
//!
//! - `%` = zero or more characters (greedy; we use backtracking).
//! - `_` = exactly one character.
//! - `\x` = literal `x` (escape preserved). Trailing `\` throws.
//! - Any other character = itself. The regex special-char escaping in TS
//!   is irrelevant for a literal matcher — we just compare codepoints.
//!
//! The TS short-circuit for patterns without any special char is
//! preserved: we return an equality predicate (optionally
//! case-insensitive) to avoid the backtracking engine for the common
//! case. Matches the TS shape 1:1.

use zero_cache_types::value::Value;

/// TS `'i' | ''` literal union.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LikeFlags {
    /// Case sensitive — TS `''`.
    None,
    /// Case insensitive — TS `'i'`.
    IgnoreCase,
}

/// Predicate over a non-null, non-undefined [`Value`]. Mirrors TS
/// `SimplePredicateNoNull = (rhs: NonNullValue) => boolean`.
///
/// The predicate panics (matching TS `assertString`) if the argument is
/// not a string — callers must upcast only when the AST guarantees
/// string operands.
pub type LikePredicate = Box<dyn Fn(&Value) -> bool + Send + Sync>;

/// TS `getLikePredicate(pattern, flags)`.
///
/// `pattern` must be a non-null, non-undefined [`Value`] whose inner
/// JSON is a string (TS's `assertString` guarantees the same). Panics
/// with the TS message otherwise.
pub fn get_like_predicate(pattern: &Value, flags: LikeFlags) -> LikePredicate {
    // TS: `String(pattern)` — pattern is typed as NonNullValue so this
    // normally cannot be null. We accept any JSON that stringifies, but
    // the common path is `JsonValue::String`.
    let pat_string = value_to_string_like_ts(pattern);
    let matcher = get_like_op(&pat_string, flags);
    Box::new(move |lhs: &Value| {
        let s = assert_string(lhs);
        matcher(s)
    })
}

fn assert_string(v: &Value) -> &str {
    match v {
        Some(serde_json::Value::String(s)) => s.as_str(),
        _ => panic!("expected string value, got: {:?}", v),
    }
}

/// Port of TS `String(pattern)` — we stringify JS-style. Only string
/// inputs hit the fast path; numeric / bool inputs fall back to their
/// `to_string()` rendering which matches JS for common cases.
fn value_to_string_like_ts(v: &Value) -> String {
    match v {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::Bool(b)) => b.to_string(),
        Some(serde_json::Value::Null) => "null".to_string(),
        None => "undefined".to_string(),
        Some(other) => other.to_string(),
    }
}

fn get_like_op(pattern: &str, flags: LikeFlags) -> Box<dyn Fn(&str) -> bool + Send + Sync> {
    // Branch: no wildcard / escape → simple equality.
    if !pattern.chars().any(|c| c == '_' || c == '%' || c == '\\') {
        match flags {
            LikeFlags::IgnoreCase => {
                let rhs_lower = pattern.to_lowercase();
                return Box::new(move |lhs: &str| lhs.to_lowercase() == rhs_lower);
            }
            LikeFlags::None => {
                let rhs = pattern.to_string();
                return Box::new(move |lhs: &str| lhs == rhs);
            }
        }
    }
    // Branch: pattern contains special chars — compile pattern tokens.
    let tokens = compile_tokens(pattern);
    let ci = matches!(flags, LikeFlags::IgnoreCase);
    Box::new(move |lhs: &str| match_tokens(&tokens, lhs, ci))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// `%` — matches zero or more chars.
    Any,
    /// `_` — matches exactly one char.
    One,
    /// A literal character.
    Lit(char),
}

/// Compile a LIKE pattern into a flat list of [`Token`]s. Collapses
/// runs of consecutive `%` into a single `Any` for matcher efficiency.
///
/// Panics if the pattern ends in a trailing `\` — matches TS's
/// `throw new Error('LIKE pattern must not end with escape character')`.
fn compile_tokens(pattern: &str) -> Vec<Token> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out: Vec<Token> = Vec::with_capacity(chars.len());
    let mut i = 0usize;
    while i < chars.len() {
        match chars[i] {
            '%' => {
                // Collapse consecutive %s.
                if !matches!(out.last(), Some(Token::Any)) {
                    out.push(Token::Any);
                }
                i += 1;
            }
            '_' => {
                out.push(Token::One);
                i += 1;
            }
            '\\' => {
                // Branch: trailing backslash → error.
                if i == chars.len() - 1 {
                    panic!("LIKE pattern must not end with escape character");
                }
                // Branch: `\x` for any x → literal x.
                i += 1;
                out.push(Token::Lit(chars[i]));
                i += 1;
            }
            c => {
                out.push(Token::Lit(c));
                i += 1;
            }
        }
    }
    out
}

/// Match a list of [`Token`]s against `input`. Case insensitive if `ci`.
///
/// Algorithm: classic two-pointer backtracking for `%`.
fn match_tokens(tokens: &[Token], input: &str, ci: bool) -> bool {
    let input_chars: Vec<char> = if ci {
        input.chars().flat_map(|c| c.to_lowercase()).collect()
    } else {
        input.chars().collect()
    };
    let toks: Vec<Token> = if ci {
        tokens
            .iter()
            .map(|t| match t {
                Token::Lit(c) => Token::Lit(c.to_lowercase().next().unwrap_or(*c)),
                Token::Any => Token::Any,
                Token::One => Token::One,
            })
            .collect()
    } else {
        tokens.to_vec()
    };

    let m = toks.len();
    let n = input_chars.len();

    // i iterates tokens, j iterates input. star/matched track the
    // last `%` rewind point.
    let mut i = 0usize;
    let mut j = 0usize;
    let mut star: Option<usize> = None;
    let mut matched: usize = 0;

    while j < n {
        if i < m {
            match &toks[i] {
                Token::One => {
                    // `_` — matches any single character.
                    i += 1;
                    j += 1;
                    continue;
                }
                Token::Lit(c) => {
                    if input_chars[j] == *c {
                        i += 1;
                        j += 1;
                        continue;
                    }
                }
                Token::Any => {
                    star = Some(i);
                    matched = j;
                    i += 1;
                    continue;
                }
            }
        }
        // Mismatch or out-of-tokens: backtrack to last `%` if any.
        if let Some(s) = star {
            i = s + 1;
            matched += 1;
            j = matched;
        } else {
            return false;
        }
    }

    // Consume trailing `%`s — they match the empty remainder.
    while i < m && matches!(toks[i], Token::Any) {
        i += 1;
    }
    i == m
}

#[cfg(test)]
mod tests {
    //! Branch coverage:
    //! - `get_like_op` no-wildcard branch (case sensitive + insensitive).
    //! - `get_like_op` pattern compilation branch.
    //! - Token compilation: `%`, `_`, `\x`, literals, collapse of `%%`.
    //! - Trailing `\` panics.
    //! - `match_tokens`:
    //!     * empty pattern on empty input → true
    //!     * empty pattern on non-empty input → false
    //!     * `_` matches single char
    //!     * `%` matches empty
    //!     * `%` matches any tail
    //!     * `a%b` matches across characters
    //!     * `_b` at start
    //!     * case-insensitive match
    //!     * escape: `\%` must match literal `%`
    //! - `get_like_predicate` — non-string value panics.

    use super::*;
    use serde_json::json;

    fn pat(s: &str) -> Value {
        Some(json!(s))
    }

    // Branch: no wildcard — case sensitive.
    #[test]
    fn no_wildcard_case_sensitive() {
        let pred = get_like_predicate(&pat("abc"), LikeFlags::None);
        assert!(pred(&pat("abc")));
        assert!(!pred(&pat("ABC")));
        assert!(!pred(&pat("abcd")));
    }

    // Branch: no wildcard — case insensitive.
    #[test]
    fn no_wildcard_case_insensitive() {
        let pred = get_like_predicate(&pat("ABC"), LikeFlags::IgnoreCase);
        assert!(pred(&pat("abc")));
        assert!(pred(&pat("ABC")));
        assert!(!pred(&pat("abcd")));
    }

    // Branch: `%` matches empty.
    #[test]
    fn percent_matches_empty() {
        let pred = get_like_predicate(&pat("%"), LikeFlags::None);
        assert!(pred(&pat("")));
        assert!(pred(&pat("foo")));
    }

    // Branch: `a%` matches anything starting with `a`.
    #[test]
    fn prefix_wildcard() {
        let pred = get_like_predicate(&pat("a%"), LikeFlags::None);
        assert!(pred(&pat("a")));
        assert!(pred(&pat("abc")));
        assert!(!pred(&pat("b")));
    }

    // Branch: `%b` matches anything ending in `b`.
    #[test]
    fn suffix_wildcard() {
        let pred = get_like_predicate(&pat("%b"), LikeFlags::None);
        assert!(pred(&pat("b")));
        assert!(pred(&pat("ab")));
        assert!(!pred(&pat("ba")));
    }

    // Branch: `a%b` — middle wildcard.
    #[test]
    fn middle_wildcard() {
        let pred = get_like_predicate(&pat("a%b"), LikeFlags::None);
        assert!(pred(&pat("ab")));
        assert!(pred(&pat("axxxxb")));
        assert!(!pred(&pat("ax")));
        assert!(!pred(&pat("xb")));
    }

    // Branch: `_` matches exactly one char.
    #[test]
    fn underscore_any_single() {
        let pred = get_like_predicate(&pat("_b"), LikeFlags::None);
        assert!(pred(&pat("ab")));
        assert!(pred(&pat("xb")));
        assert!(!pred(&pat("b")));
        assert!(!pred(&pat("xxb")));
    }

    // Branch: case-insensitive with wildcard.
    #[test]
    fn wildcard_case_insensitive() {
        let pred = get_like_predicate(&pat("A%"), LikeFlags::IgnoreCase);
        assert!(pred(&pat("alpha")));
        assert!(pred(&pat("ALPHA")));
        assert!(!pred(&pat("beta")));
    }

    // Branch: escape `\%` matches literal `%`.
    #[test]
    fn escape_percent_literal() {
        let pred = get_like_predicate(&pat("a\\%b"), LikeFlags::None);
        assert!(pred(&pat("a%b")));
        assert!(!pred(&pat("axxb")));
    }

    // Branch: escape `\_` matches literal `_`.
    #[test]
    fn escape_underscore_literal() {
        let pred = get_like_predicate(&pat("a\\_b"), LikeFlags::None);
        assert!(pred(&pat("a_b")));
        assert!(!pred(&pat("axb")));
    }

    // Branch: trailing backslash panics.
    #[test]
    #[should_panic(expected = "must not end with escape")]
    fn trailing_backslash_panics() {
        let _ = get_like_predicate(&pat("a\\"), LikeFlags::None);
    }

    // Branch: empty pattern matches only empty.
    #[test]
    fn empty_pattern() {
        let pred = get_like_predicate(&pat(""), LikeFlags::None);
        assert!(pred(&pat("")));
        assert!(!pred(&pat("a")));
    }

    // Branch: collapse `%%` — equivalent to single `%`.
    #[test]
    fn double_percent_collapses() {
        let pred = get_like_predicate(&pat("%%"), LikeFlags::None);
        assert!(pred(&pat("")));
        assert!(pred(&pat("anything")));
    }

    // Branch: regex-special characters in pattern treated as literals.
    #[test]
    fn regex_specials_are_literal() {
        // The pattern uses chars that would be special in regex but are
        // literal in LIKE: `.`, `+`, `*`, `[`, `]`, `(`, `)`, `|`, etc.
        let pred = get_like_predicate(&pat("a.b"), LikeFlags::None);
        assert!(pred(&pat("a.b")));
        assert!(!pred(&pat("axb")));

        let pred2 = get_like_predicate(&pat("(x)"), LikeFlags::None);
        assert!(pred2(&pat("(x)")));
    }

    // Branch: non-string value panics (assertString).
    #[test]
    #[should_panic(expected = "expected string value")]
    fn non_string_value_panics() {
        let pred = get_like_predicate(&pat("a%"), LikeFlags::None);
        let _ = pred(&Some(json!(42)));
    }
}
