//! Port of `packages/zql/src/query/escape-like.ts`.
//!
//! Single export: [`escape_like`] — escape `%` and `_` for a SQL LIKE
//! pattern by prefixing them with `\`.

/// TS `escapeLike(val)` — escape `%` and `_` so they are treated as
/// literal characters in a SQL LIKE pattern.
///
/// Mirrors `val.replace(/[%_]/g, '\\$&')`.
pub fn escape_like(val: &str) -> String {
    let mut out = String::with_capacity(val.len());
    for ch in val.chars() {
        if ch == '%' || ch == '_' {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Branch: input contains neither `%` nor `_`.
    #[test]
    fn no_special_chars_returns_unchanged() {
        assert_eq!(escape_like("hello"), "hello");
    }

    // Branch: input is empty.
    #[test]
    fn empty_string_returns_empty() {
        assert_eq!(escape_like(""), "");
    }

    // Branch: input has `%`.
    #[test]
    fn percent_is_escaped() {
        assert_eq!(escape_like("50%"), "50\\%");
    }

    // Branch: input has `_`.
    #[test]
    fn underscore_is_escaped() {
        assert_eq!(escape_like("a_b"), "a\\_b");
    }

    // Branch: input has both.
    #[test]
    fn mixed_specials_all_escaped() {
        assert_eq!(escape_like("%_%"), "\\%\\_\\%");
    }

    // Branch: non-ASCII passed through.
    #[test]
    fn unicode_passthrough() {
        assert_eq!(escape_like("café_%"), "café\\_\\%");
    }
}
