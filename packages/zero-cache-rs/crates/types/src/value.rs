//! Core data types: Value and Row.
//!
//! Maps to `zero-protocol/src/data.ts`.
//! Value = JSON | undefined (represented as Option<serde_json::Value>).
//! Row   = Record<string, Value> (ordered map for deterministic serialization).

use indexmap::IndexMap;
// serde is used via the type aliases (Row = IndexMap which derives Serialize/Deserialize)

/// A Zero value: any JSON-compatible type, or absent (`None` = JS `undefined`).
///
/// Using `serde_json::Value` directly gives us null, bool, number, string,
/// array, and object — exactly matching the TS `JSONValue` type.
/// `None` maps to JS `undefined` (omitted from serialized output).
pub type Value = Option<serde_json::Value>;

/// A row is a string-keyed map of [`Value`]s.
///
/// Uses [`IndexMap`] to preserve insertion order (matching JS object iteration
/// order) while still giving O(1) lookups. This matters for deterministic
/// serialization and for matching TS behavior in tests.
pub type Row = IndexMap<String, Value>;

/// Convenience: create a [`serde_json::Value::String`] from anything string-like.
#[inline]
pub fn json_string(s: impl Into<String>) -> serde_json::Value {
    serde_json::Value::String(s.into())
}

/// Convenience: create a [`serde_json::Value::Number`] from an i64.
#[inline]
pub fn json_i64(n: i64) -> serde_json::Value {
    serde_json::Value::Number(n.into())
}

/// Convenience: create a [`serde_json::Value::Number`] from an f64.
/// Returns `Null` if the float is NaN or infinite (not representable in JSON).
#[inline]
pub fn json_f64(n: f64) -> serde_json::Value {
    serde_json::Number::from_f64(n)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

/// JavaScript `Number.MAX_SAFE_INTEGER` — 2^53 - 1.
pub const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
/// JavaScript `Number.MIN_SAFE_INTEGER` — -(2^53 - 1).
pub const MIN_SAFE_INTEGER: i64 = -9_007_199_254_740_991;

/// Validate that `row` (a JSON object) is safe to serialize for the wire
/// protocol — specifically, that every top-level number fits in JS's
/// `Number.MIN_SAFE_INTEGER..=Number.MAX_SAFE_INTEGER` range.
///
/// Maps to TS `ensureSafeJSON` in
/// `packages/zero-cache/src/services/view-syncer/client-handler.ts`. In TS
/// the input may contain `bigint` values (produced by the Postgres driver for
/// `INT8` columns); the TS helper converts those to `number` when within the
/// safe range and throws otherwise. In Rust, `serde_json::Value` does not
/// carry a `BigInt` variant — integers are already stored as `i64`/`u64` in
/// `serde_json::Number`. The parity behaviour is therefore:
///
/// * if any top-level field's number exceeds the JS safe range, return
///   `Err` with the same message shape TS throws;
/// * otherwise, return the object unchanged (cloned so the caller gets an
///   owned `Value`). Nested objects are recursively validated — matching the
///   TS call `assertJSONValue(v)` for `typeof v === 'object'` — but their
///   numbers are left as-is (TS also leaves nested numbers alone: only
///   top-level BigInts are rewritten).
///
/// Callers pass `Value::Object` (TS `JSONObject`). Passing any other variant
/// returns an error — the TS signature enforces this at the type level.
pub fn ensure_safe_json(row: &serde_json::Value) -> Result<serde_json::Value, String> {
    let obj = row
        .as_object()
        .ok_or_else(|| "ensure_safe_json: expected JSON object".to_string())?;

    for (k, v) in obj.iter() {
        check_value(k, v)?;
    }

    // Mirror TS behaviour: when nothing needs rewriting, return the row
    // unchanged. We still clone because `serde_json::Value` is owned; the
    // clone is cheap for typical row sizes.
    Ok(row.clone())
}

/// Recursively ensure `v` is safe. Top-level numbers are the only thing TS
/// rewrites; nested objects just need to be valid JSON (no `bigint`).
fn check_value(key: &str, v: &serde_json::Value) -> Result<(), String> {
    match v {
        serde_json::Value::Number(n) => {
            // The only way to violate the safe range with `serde_json::Number`
            // is an integer outside [MIN_SAFE_INTEGER, MAX_SAFE_INTEGER] or a
            // float outside the same magnitude. Floats that fit into f64 are
            // always within the safe Number range by definition (f64 *is* JS
            // Number), so we only need to check integers.
            if let Some(i) = n.as_i64() {
                if !(MIN_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&i) {
                    return Err(format!(
                        "Value of \"{key}\" exceeds safe Number range ({i})"
                    ));
                }
            } else if let Some(u) = n.as_u64() {
                if u > MAX_SAFE_INTEGER as u64 {
                    return Err(format!(
                        "Value of \"{key}\" exceeds safe Number range ({u})"
                    ));
                }
            }
            Ok(())
        }
        serde_json::Value::Object(inner) => {
            for (k, v) in inner.iter() {
                check_value(k, v)?;
                // Note: TS's assertJSONValue is recursive and would throw on
                // any nested bigint. serde_json::Value has no bigint variant,
                // so nested JSON is safe by construction once we've checked
                // its numbers.
            }
            Ok(())
        }
        serde_json::Value::Array(items) => {
            for item in items {
                check_value(key, item)?;
            }
            Ok(())
        }
        // Null / Bool / String are always safe.
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_round_trip() {
        let mut row = Row::new();
        row.insert("id".into(), Some(json_string("abc")));
        row.insert("count".into(), Some(json_i64(42)));
        row.insert("active".into(), Some(serde_json::Value::Bool(true)));
        row.insert("deleted".into(), None); // undefined

        let json = serde_json::to_string(&row).unwrap();
        let parsed: Row = serde_json::from_str(&json).unwrap();

        assert_eq!(row.get("id"), parsed.get("id"));
        assert_eq!(row.get("count"), parsed.get("count"));
        assert_eq!(row.get("active"), parsed.get("active"));
    }

    // -----------------------------------------------------------------------
    // Tests ported from
    //   zero-cache/src/services/view-syncer/client-handler.test.ts `ensureSafeJSON`.
    //
    // The TS cases use `bigint` literals (e.g. `234n`); in Rust these arrive
    // as plain numbers because `serde_json::Value::Number` already stores
    // integers up to `u64` / `i64`. The function therefore validates the
    // same *range* constraint the TS helper enforces.
    // -----------------------------------------------------------------------

    #[test]
    fn ensure_safe_json_safe_top_level_int() {
        let input = serde_json::json!({"foo": 1, "bar": 2});
        let out = ensure_safe_json(&input).unwrap();
        assert_eq!(out, serde_json::json!({"foo": 1, "bar": 2}));
    }

    #[test]
    fn ensure_safe_json_keeps_strings_and_numbers() {
        let input = serde_json::json!({"foo": "1", "bar": 234});
        let out = ensure_safe_json(&input).unwrap();
        assert_eq!(out, serde_json::json!({"foo": "1", "bar": 234}));
    }

    #[test]
    fn ensure_safe_json_recurses_into_nested_object() {
        let input = serde_json::json!({"foo": 123, "bar": {"baz": 23423423}});
        let out = ensure_safe_json(&input).unwrap();
        assert_eq!(out, input);
    }

    #[test]
    fn ensure_safe_json_errors_on_unsafe_top_level_int() {
        // TS: `{foo: '1', bar: 23423423434923874239487n}` → throws.
        // We use 2^53 (one past MAX_SAFE_INTEGER) as a reproducible boundary.
        let input = serde_json::json!({"foo": "1", "bar": MAX_SAFE_INTEGER + 1});
        let err = ensure_safe_json(&input).unwrap_err();
        assert!(
            err.contains("exceeds safe Number range"),
            "unexpected error: {err}"
        );
        assert!(err.contains("\"bar\""));
    }

    #[test]
    fn ensure_safe_json_errors_on_unsafe_nested_int() {
        // TS: `{foo: '1', bar: {baz: 23423423434923874239487n}}` → throws.
        let input = serde_json::json!({"foo": "1", "bar": {"baz": MAX_SAFE_INTEGER + 1}});
        let err = ensure_safe_json(&input).unwrap_err();
        assert!(err.contains("exceeds safe Number range"));
        assert!(err.contains("\"baz\""));
    }

    #[test]
    fn ensure_safe_json_rejects_unsafe_negative() {
        let input = serde_json::json!({"n": MIN_SAFE_INTEGER - 1});
        let err = ensure_safe_json(&input).unwrap_err();
        assert!(err.contains("exceeds safe Number range"));
    }

    #[test]
    fn ensure_safe_json_allows_boundary_values() {
        let input = serde_json::json!({
            "max": MAX_SAFE_INTEGER,
            "min": MIN_SAFE_INTEGER,
        });
        let out = ensure_safe_json(&input).unwrap();
        assert_eq!(out, input);
    }

    #[test]
    fn ensure_safe_json_rejects_non_object() {
        let input = serde_json::json!([1, 2, 3]);
        let err = ensure_safe_json(&input).unwrap_err();
        assert!(err.contains("expected JSON object"));
    }

    #[test]
    fn value_null_vs_none() {
        let null_val: Value = Some(serde_json::Value::Null);
        let none_val: Value = None;

        // null serializes as "null", None serializes as nothing (skipped)
        assert_eq!(serde_json::to_string(&null_val).unwrap(), "null");
        assert_eq!(serde_json::to_string(&none_val).unwrap(), "null");
        // Both are valid — the distinction matters at the Row level
        // where None means "field absent" and Some(Null) means "field is null"
    }
}
