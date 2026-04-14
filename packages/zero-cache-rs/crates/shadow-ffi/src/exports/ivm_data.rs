//! Shadow of `packages/zql/src/ivm/data.ts`.
//!
//! Exposes the pure helpers (`compare_values`, `values_equal`,
//! `normalize_undefined`, `make_comparator`) to TS so the shadow harness
//! can diff them against the stock implementation. `Node` is not exposed
//! — relationship factories cross the FFI boundary awkwardly, and
//! consumers of `Node` shape will shadow at a higher level.
//!
//! Argument shape: all values come in as `serde_json::Value` (from the
//! napi-rs `serde-json` feature). Internal conversion to
//! `zero_cache_types::value::Value` (`Option<serde_json::Value>`) is done
//! in the wrapper: `Null` → `Some(Null)`, missing arg → `None`.

use serde_json::Value as JsonValue;

use zero_cache_sync_worker::ivm::data::{compare_values, normalize_undefined, values_equal};
use zero_cache_types::value::{Row, Value};

/// Wrap a JSON value as a [`Value`]. `null` becomes `Some(Null)`; there
/// is no way to produce `None` from a single arg — callers that need
/// `undefined` semantics must use [`ivm_compare_values_opt`].
fn to_value(v: JsonValue) -> Value {
    Some(v)
}

/// Catch the type-mismatch panic from [`compare_values`] and surface it as
/// a `napi::Error` so the FFI boundary doesn't abort the Node process.
fn safe_compare(a: &Value, b: &Value) -> napi::Result<i32> {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    match catch_unwind(AssertUnwindSafe(|| compare_values(a, b))) {
        Ok(cmp) => Ok(match cmp {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "compareValues: type mismatch".to_string());
            Err(napi::Error::from_reason(msg))
        }
    }
}

/// Shadow of TS `compareValues(a, b)`.
///
/// Returns `-1 / 0 / 1` to match TS's signed-integer contract. Cross-type
/// comparison surfaces as a thrown JS `Error` (TS does the same).
#[napi(js_name = "ivm_data_compare_values")]
pub fn ivm_data_compare_values(a: JsonValue, b: JsonValue) -> napi::Result<i32> {
    safe_compare(&to_value(a), &to_value(b))
}

/// Variant of [`ivm_data_compare_values`] accepting `null` to mean
/// undefined. TS's `compareValues(undefined, x)` folds `undefined` to
/// `null` internally; calling the JSON variant with `null` exercises
/// that code path.
#[napi(js_name = "ivm_data_compare_values_opt")]
pub fn ivm_data_compare_values_opt(
    a: Option<JsonValue>,
    b: Option<JsonValue>,
) -> napi::Result<i32> {
    safe_compare(&a, &b)
}

/// Shadow of TS `valuesEqual(a, b)`. Note NULL/undefined never equal
/// themselves here — required for join predicates.
#[napi(js_name = "ivm_data_values_equal")]
pub fn ivm_data_values_equal(a: Option<JsonValue>, b: Option<JsonValue>) -> bool {
    values_equal(&a, &b)
}

/// Shadow of TS `normalizeUndefined(v)`. Absent (`None`) and `null` both
/// return `JsonValue::Null`.
#[napi(js_name = "ivm_data_normalize_undefined")]
pub fn ivm_data_normalize_undefined(v: Option<JsonValue>) -> JsonValue {
    normalize_undefined(&v)
}

/// Shadow of TS `makeComparator(order, reverse)` applied to two rows.
///
/// The TS side exposes a factory; we expose an eagerly-evaluated form
/// (build + compare in one call) because returning a closure across FFI
/// adds complexity for no real test coverage benefit: the diff harness
/// calls the comparator once per row pair anyway.
///
/// `order` wire format: `[[field, 'asc'|'desc'], …]` — matches TS
/// `Ordering`. Returns `-1 / 0 / 1`.
#[napi(js_name = "ivm_data_compare_rows")]
pub fn ivm_data_compare_rows(
    order: Vec<(String, String)>,
    reverse: bool,
    a: serde_json::Map<String, JsonValue>,
    b: serde_json::Map<String, JsonValue>,
) -> napi::Result<i32> {
    use std::cmp::Ordering as CmpOrdering;
    use zero_cache_types::ast::{Direction, OrderPart};

    // Convert the wire-level order to AST Ordering.
    let ast_order: Vec<OrderPart> = order
        .into_iter()
        .map(|(field, dir)| {
            let d = match dir.as_str() {
                "asc" => Direction::Asc,
                "desc" => Direction::Desc,
                _ => {
                    return Err(napi::Error::from_reason(format!(
                        "ivm_data_compare_rows: direction must be 'asc' or 'desc', got {dir:?}"
                    )));
                }
            };
            Ok((field, d))
        })
        .collect::<napi::Result<_>>()?;

    // Convert JSON maps to Row (Option<JsonValue> per field).
    let to_row = |m: serde_json::Map<String, JsonValue>| -> Row {
        let mut r = Row::new();
        for (k, v) in m {
            r.insert(k, Some(v));
        }
        r
    };
    let row_a = to_row(a);
    let row_b = to_row(b);

    let cmp =
        zero_cache_sync_worker::ivm::data::make_comparator(ast_order, reverse)(&row_a, &row_b);
    Ok(match cmp {
        CmpOrdering::Less => -1,
        CmpOrdering::Equal => 0,
        CmpOrdering::Greater => 1,
    })
}
