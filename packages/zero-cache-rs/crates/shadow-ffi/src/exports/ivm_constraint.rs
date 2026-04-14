//! Shadow of `packages/zql/src/ivm/constraint.ts`.
//!
//! Exposes every public function. Constraints cross the FFI as
//! `serde_json::Map<String, serde_json::Value>`; primary keys as
//! `Vec<String>`; Conditions as `serde_json::Value` (deserialised into
//! the Rust AST on the other side).
//!
//! `SetOfConstraint` is TS-testing-only and is not ported.

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use zero_cache_sync_worker::ivm::constraint::{
    Constraint, constraint_matches_primary_key, constraint_matches_row, constraints_are_compatible,
    key_matches_primary_key, primary_key_constraint_from_filters, pull_simple_and_components,
};
use zero_cache_types::ast::Condition;
use zero_cache_types::primary_key::PrimaryKey;
use zero_cache_types::value::{Row, Value};

fn json_map_to_constraint(m: serde_json::Map<String, JsonValue>) -> Constraint {
    let mut c: Constraint = IndexMap::new();
    for (k, v) in m {
        // JS `null` stays as `Some(Null)`; a truly absent key is `None`,
        // but JSON objects can't express that — absent keys never made it
        // into the map on the TS side either.
        c.insert(k, Some(v));
    }
    c
}

fn json_map_to_row(m: serde_json::Map<String, JsonValue>) -> Row {
    let mut r = Row::new();
    for (k, v) in m {
        r.insert(k, Some(v));
    }
    r
}

/// Shadow of TS `constraintMatchesRow(constraint, row)`.
#[napi(js_name = "ivm_constraint_matches_row")]
pub fn ivm_constraint_matches_row(
    constraint: serde_json::Map<String, JsonValue>,
    row: serde_json::Map<String, JsonValue>,
) -> bool {
    let c = json_map_to_constraint(constraint);
    let r = json_map_to_row(row);
    constraint_matches_row(&c, &r)
}

/// Shadow of TS `constraintsAreCompatible(left, right)`.
#[napi(js_name = "ivm_constraints_are_compatible")]
pub fn ivm_constraints_are_compatible(
    left: serde_json::Map<String, JsonValue>,
    right: serde_json::Map<String, JsonValue>,
) -> bool {
    let l = json_map_to_constraint(left);
    let r = json_map_to_constraint(right);
    constraints_are_compatible(&l, &r)
}

/// Shadow of TS `constraintMatchesPrimaryKey(constraint, primary)`.
#[napi(js_name = "ivm_constraint_matches_primary_key")]
pub fn ivm_constraint_matches_primary_key(
    constraint: serde_json::Map<String, JsonValue>,
    primary: Vec<String>,
) -> bool {
    let c = json_map_to_constraint(constraint);
    let pk = PrimaryKey::new(primary);
    constraint_matches_primary_key(&c, &pk)
}

/// Shadow of TS `keyMatchesPrimaryKey(key, primary)`.
#[napi(js_name = "ivm_key_matches_primary_key")]
pub fn ivm_key_matches_primary_key(key: Vec<String>, primary: Vec<String>) -> bool {
    let pk = PrimaryKey::new(primary);
    key_matches_primary_key(key.iter().map(String::as_str), &pk)
}

/// Shadow of TS `pullSimpleAndComponents(condition)`. Returns a
/// JSON-array of Condition objects so the TS test can diff against
/// its own output directly.
#[napi(js_name = "ivm_pull_simple_and_components")]
pub fn ivm_pull_simple_and_components(condition: JsonValue) -> napi::Result<JsonValue> {
    let cond: Condition = serde_json::from_value(condition).map_err(|e| {
        napi::Error::from_reason(format!("pull_simple_and_components: bad condition: {e}"))
    })?;
    let out = pull_simple_and_components(&cond);
    serde_json::to_value(&out).map_err(|e| {
        napi::Error::from_reason(format!("pull_simple_and_components: serialise: {e}"))
    })
}

/// Shadow of TS `primaryKeyConstraintFromFilters(condition?, primary)`.
/// Returns `null` when the TS version returns `undefined`.
#[napi(js_name = "ivm_primary_key_constraint_from_filters")]
pub fn ivm_primary_key_constraint_from_filters(
    condition: Option<JsonValue>,
    primary: Vec<String>,
) -> napi::Result<Option<JsonValue>> {
    let cond = match condition {
        Some(c) => Some(serde_json::from_value::<Condition>(c).map_err(|e| {
            napi::Error::from_reason(format!(
                "primary_key_constraint_from_filters: bad condition: {e}"
            ))
        })?),
        None => None,
    };
    let pk = PrimaryKey::new(primary);
    let out = primary_key_constraint_from_filters(cond.as_ref(), &pk);
    match out {
        None => Ok(None),
        Some(c) => {
            // Project IndexMap<String, Value> → JSON object. Value=None
            // collapses to JSON null (same as TS undefined-via-JSON).
            let mut m = serde_json::Map::new();
            for (k, v) in c.into_iter() {
                let v_json = match v {
                    Some(j) => j,
                    None => JsonValue::Null,
                };
                m.insert(k, v_json);
            }
            Ok(Some(JsonValue::Object(m)))
        }
    }
}

/// Hidden: convert a [`Value`] map to a JSON object (used by tests).
#[allow(dead_code)]
fn _touch_value_for_clippy(_v: Value) {}
