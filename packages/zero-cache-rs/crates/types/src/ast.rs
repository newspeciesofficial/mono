//! ZQL Abstract Syntax Tree types.
//!
//! Maps to `zero-protocol/src/ast.ts`.
//! These types define the wire format for queries sent between client and server.
//! The serde attributes ensure JSON round-trip compatibility with the TS implementation.

use crate::value::Row;
use serde::{Deserialize, Serialize};

pub const SUBQ_PREFIX: &str = "zsubq_";

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Asc,
    Desc,
}

/// A single order-by element: `[field, direction]`.
/// Wire format: `["colName", "asc"]` (JSON tuple).
pub type OrderPart = (String, Direction);

/// Full ordering: list of `[field, direction]` pairs.
pub type Ordering = Vec<OrderPart>;

// ---------------------------------------------------------------------------
// System tag
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum System {
    Permissions,
    Client,
    Test,
}

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SimpleOperator {
    #[serde(rename = "=")]
    Eq,
    #[serde(rename = "!=")]
    Ne,
    IS,
    #[serde(rename = "IS NOT")]
    IsNot,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = "<=")]
    Le,
    #[serde(rename = ">=")]
    Ge,
    LIKE,
    #[serde(rename = "NOT LIKE")]
    NotLike,
    ILIKE,
    #[serde(rename = "NOT ILIKE")]
    NotIlike,
    IN,
    #[serde(rename = "NOT IN")]
    NotIn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CorrelatedSubqueryOp {
    EXISTS,
    #[serde(rename = "NOT EXISTS")]
    NotExists,
}

// ---------------------------------------------------------------------------
// Value positions (condition operands)
// ---------------------------------------------------------------------------

/// A literal value in a condition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LiteralValue {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Array(Vec<ScalarLiteral>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScalarLiteral {
    String(String),
    Number(f64),
    Bool(bool),
}

/// Parameter anchor: where the runtime value comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ParameterAnchor {
    AuthData,
    PreMutationRow,
}

/// Parameter field: single name or dotted path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ParameterField {
    Single(String),
    Path(Vec<String>),
}

/// A value reference in a condition: literal, column, or static parameter.
/// Discriminated by `"type"` field in JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ValuePosition {
    #[serde(rename = "literal")]
    Literal { value: LiteralValue },
    #[serde(rename = "column")]
    Column { name: String },
    #[serde(rename = "static")]
    Static {
        anchor: ParameterAnchor,
        field: ParameterField,
    },
}

/// Right-hand side of a condition: no column references allowed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NonColumnValue {
    #[serde(rename = "literal")]
    Literal { value: LiteralValue },
    #[serde(rename = "static")]
    Static {
        anchor: ParameterAnchor,
        field: ParameterField,
    },
}

// ---------------------------------------------------------------------------
// Conditions (recursive)
// ---------------------------------------------------------------------------

/// A query condition. Discriminated union on `"type"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Condition {
    #[serde(rename = "simple")]
    Simple {
        op: SimpleOperator,
        left: ValuePosition,
        right: NonColumnValue,
    },
    #[serde(rename = "and")]
    And { conditions: Vec<Condition> },
    #[serde(rename = "or")]
    Or { conditions: Vec<Condition> },
    #[serde(rename = "correlatedSubquery")]
    CorrelatedSubquery {
        related: Box<CorrelatedSubquery>,
        op: CorrelatedSubqueryOp,
        #[serde(skip_serializing_if = "Option::is_none")]
        flip: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        scalar: Option<bool>,
    },
}

// ---------------------------------------------------------------------------
// Compound keys and correlation
// ---------------------------------------------------------------------------

/// Non-empty list of field names for correlation keys.
pub type CompoundKey = Vec<String>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Correlation {
    pub parent_field: CompoundKey,
    pub child_field: CompoundKey,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CorrelatedSubquery {
    pub correlation: Correlation,
    pub subquery: Box<AST>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<System>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hidden: Option<bool>,
}

// ---------------------------------------------------------------------------
// Bound (cursor position)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bound {
    pub row: Row,
    pub exclusive: bool,
}

// ---------------------------------------------------------------------------
// AST (top-level query)
// ---------------------------------------------------------------------------

/// The root query AST node. Wire format must match `zero-protocol/src/ast.ts`.
///
/// Note: the `where` field uses `#[serde(rename = "where")]` because "where"
/// is a Rust keyword.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AST {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<Box<Condition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related: Option<Vec<CorrelatedSubquery>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<Bound>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(rename = "orderBy", skip_serializing_if = "Option::is_none")]
    pub order_by: Option<Ordering>,
}

// ---------------------------------------------------------------------------
// Static parameter binding
// ---------------------------------------------------------------------------

/// Static query parameters for binding `{type: "static"}` values in AST conditions.
/// Maps to TS `StaticQueryParameters` from `zql/src/builder/builder.ts`.
pub struct StaticQueryParameters {
    /// Decoded JWT auth data (e.g., `{sub: "user123", email: "..."}`).
    pub auth_data: serde_json::Value,
    /// Pre-mutation row data (used for write authorization).
    pub pre_mutation_row: Option<serde_json::Value>,
}

/// Bind static parameters in an AST, replacing `{type: "static", anchor, field}`
/// with `{type: "literal", value: resolvedValue}`.
///
/// Maps to `bindStaticParameters()` in `zql/src/builder/builder.ts:145-201`.
/// This must be called BEFORE the AST is used for query execution.
pub fn bind_static_parameters(ast: &AST, params: &StaticQueryParameters) -> AST {
    fn visit(node: &AST, params: &StaticQueryParameters) -> AST {
        AST {
            schema: node.schema.clone(),
            table: node.table.clone(),
            alias: node.alias.clone(),
            where_clause: node
                .where_clause
                .as_ref()
                .map(|c| Box::new(bind_condition(c, params))),
            related: node.related.as_ref().map(|subs| {
                subs.iter()
                    .map(|sq| CorrelatedSubquery {
                        correlation: sq.correlation.clone(),
                        subquery: Box::new(visit(&sq.subquery, params)),
                        system: sq.system,
                        hidden: sq.hidden,
                    })
                    .collect()
            }),
            start: node.start.clone(),
            limit: node.limit,
            order_by: node.order_by.clone(),
        }
    }

    fn bind_condition(condition: &Condition, params: &StaticQueryParameters) -> Condition {
        match condition {
            Condition::Simple { op, left, right } => Condition::Simple {
                op: *op,
                left: bind_value_position(left, params),
                right: bind_non_column_value(right, params),
            },
            Condition::And { conditions } => Condition::And {
                conditions: conditions
                    .iter()
                    .map(|c| bind_condition(c, params))
                    .collect(),
            },
            Condition::Or { conditions } => Condition::Or {
                conditions: conditions
                    .iter()
                    .map(|c| bind_condition(c, params))
                    .collect(),
            },
            Condition::CorrelatedSubquery {
                related,
                op,
                flip,
                scalar,
            } => Condition::CorrelatedSubquery {
                related: Box::new(CorrelatedSubquery {
                    correlation: related.correlation.clone(),
                    subquery: Box::new(visit(&related.subquery, params)),
                    system: related.system,
                    hidden: related.hidden,
                }),
                op: *op,
                flip: *flip,
                scalar: *scalar,
            },
        }
    }

    fn bind_value_position(value: &ValuePosition, params: &StaticQueryParameters) -> ValuePosition {
        match value {
            ValuePosition::Static { anchor, field } => {
                let resolved = resolve_parameter(anchor, field, params);
                ValuePosition::Literal {
                    value: json_to_literal_value(&resolved),
                }
            }
            other => other.clone(),
        }
    }

    fn bind_non_column_value(
        value: &NonColumnValue,
        params: &StaticQueryParameters,
    ) -> NonColumnValue {
        match value {
            NonColumnValue::Static { anchor, field } => {
                let resolved = resolve_parameter(anchor, field, params);
                NonColumnValue::Literal {
                    value: json_to_literal_value(&resolved),
                }
            }
            other => other.clone(),
        }
    }

    fn resolve_parameter(
        anchor: &ParameterAnchor,
        field: &ParameterField,
        params: &StaticQueryParameters,
    ) -> serde_json::Value {
        let anchor_data = match anchor {
            ParameterAnchor::AuthData => &params.auth_data,
            ParameterAnchor::PreMutationRow => params
                .pre_mutation_row
                .as_ref()
                .unwrap_or(&serde_json::Value::Null),
        };

        resolve_field(anchor_data, field)
    }

    fn resolve_field(data: &serde_json::Value, field: &ParameterField) -> serde_json::Value {
        match field {
            ParameterField::Single(name) => {
                data.get(name).cloned().unwrap_or(serde_json::Value::Null)
            }
            ParameterField::Path(parts) => {
                let mut current = data;
                for part in parts {
                    match current.get(part) {
                        Some(v) => current = v,
                        None => return serde_json::Value::Null,
                    }
                }
                current.clone()
            }
        }
    }

    fn json_to_literal_value(v: &serde_json::Value) -> LiteralValue {
        match v {
            serde_json::Value::String(s) => LiteralValue::String(s.clone()),
            serde_json::Value::Number(n) => LiteralValue::Number(n.as_f64().unwrap_or(0.0)),
            serde_json::Value::Bool(b) => LiteralValue::Bool(*b),
            serde_json::Value::Null => LiteralValue::Null,
            serde_json::Value::Array(arr) => LiteralValue::Array(
                arr.iter()
                    .filter_map(|v| match v {
                        serde_json::Value::String(s) => Some(ScalarLiteral::String(s.clone())),
                        serde_json::Value::Number(n) => n.as_f64().map(ScalarLiteral::Number),
                        serde_json::Value::Bool(b) => Some(ScalarLiteral::Bool(*b)),
                        _ => None,
                    })
                    .collect(),
            ),
            serde_json::Value::Object(_) => LiteralValue::Null,
        }
    }

    visit(ast, params)
}

// ---------------------------------------------------------------------------
// AST normalization (`normalizeAST`) + `mapAST` + `hashOfAST`
//
// Ports of:
//   - `zero-protocol/src/ast.ts` (`normalizeAST`, `mapAST`, `mapCondition`,
//     `transformAST`, `transformWhere`, `flattened`, `sortedRelated`,
//     `cmpCondition`, `compareValuePosition`, `cmpRelated`,
//     `compareUTF8MaybeNull`, `cmpOptionalBool`).
//   - `zero-protocol/src/query-hash.ts` (`hashOfAST`).
//   - `shared/src/hash.ts` (`h64(s) = (xxHash32(s, 0) << 32) + xxHash32(s, 1)`).
// ---------------------------------------------------------------------------

/// TS `NameMapper` surface used by `mapAST` / `mapCondition`. Takes a table
/// name (and for columns, the table owning the column) and maps to the
/// destination representation (client↔server).
pub trait NameMapper {
    fn table_name(&self, orig_table: &str) -> String;
    fn column_name(&self, orig_table: &str, orig_column: &str) -> String;
}

/// Identity mapper: returns inputs unchanged. Used by `normalize_ast` since
/// normalization is naming-preserving.
pub struct IdentityNameMapper;

impl NameMapper for IdentityNameMapper {
    fn table_name(&self, orig_table: &str) -> String {
        orig_table.to_string()
    }
    fn column_name(&self, _orig_table: &str, orig_column: &str) -> String {
        orig_column.to_string()
    }
}

/// TS `ASTTransform`. Describes the four pluggable hooks used by the
/// generic `transformAST`/`transformWhere` walker:
///   - `tableName` / `columnName` (name mapping)
///   - `related_transform` (sort or pass-through subquery list)
///   - `where_transform` (flatten or pass-through top-level `where`)
///   - `conditions_transform` (sort or pass-through nested and/or list)
struct AstTransform<'a> {
    mapper: &'a dyn NameMapper,
    related_transform: fn(Vec<CorrelatedSubquery>) -> Vec<CorrelatedSubquery>,
    where_transform: fn(&Condition) -> Option<Condition>,
    conditions_transform: fn(Vec<Condition>) -> Vec<Condition>,
}

fn transform_ast(ast: &AST, t: &AstTransform<'_>) -> AST {
    let col_name = |c: &str| t.mapper.column_name(&ast.table, c);
    let key = |table: &str, k: &CompoundKey| -> CompoundKey {
        k.iter()
            .map(|col| t.mapper.column_name(table, col))
            .collect()
    };

    // Apply `where` hook first (top-level flatten decides presence).
    let where_mapped: Option<Condition> = match ast.where_clause.as_deref() {
        Some(w) => (t.where_transform)(w),
        None => None,
    };
    let where_transformed: Option<Box<Condition>> =
        where_mapped.map(|w| Box::new(transform_where(&w, &ast.table, t)));

    let related: Option<Vec<CorrelatedSubquery>> = ast.related.as_ref().map(|rs| {
        let mapped: Vec<CorrelatedSubquery> = rs
            .iter()
            .map(|r| CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: key(&ast.table, &r.correlation.parent_field),
                    child_field: key(&r.subquery.table, &r.correlation.child_field),
                },
                hidden: r.hidden,
                subquery: Box::new(transform_ast(&r.subquery, t)),
                system: r.system,
            })
            .collect();
        (t.related_transform)(mapped)
    });

    let start = ast.start.as_ref().map(|b| Bound {
        row: {
            let mut out = Row::new();
            for (col, val) in b.row.iter() {
                out.insert(col_name(col), val.clone());
            }
            out
        },
        exclusive: b.exclusive,
    });

    let order_by = ast.order_by.as_ref().map(|o| {
        o.iter()
            .map(|(col, dir)| (col_name(col), *dir))
            .collect::<Ordering>()
    });

    AST {
        schema: ast.schema.clone(),
        table: t.mapper.table_name(&ast.table),
        alias: ast.alias.clone(),
        where_clause: where_transformed,
        related,
        start,
        limit: ast.limit,
        order_by,
    }
}

fn transform_where(where_: &Condition, table: &str, t: &AstTransform<'_>) -> Condition {
    match where_ {
        Condition::Simple { op, left, right } => {
            let new_left = match left {
                ValuePosition::Column { name } => ValuePosition::Column {
                    name: t.mapper.column_name(table, name),
                },
                other => other.clone(),
            };
            Condition::Simple {
                op: *op,
                left: new_left,
                right: right.clone(),
            }
        }
        Condition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
        } => {
            let new_correlation = Correlation {
                parent_field: related
                    .correlation
                    .parent_field
                    .iter()
                    .map(|col| t.mapper.column_name(table, col))
                    .collect(),
                child_field: related
                    .correlation
                    .child_field
                    .iter()
                    .map(|col| t.mapper.column_name(&related.subquery.table, col))
                    .collect(),
            };
            Condition::CorrelatedSubquery {
                related: Box::new(CorrelatedSubquery {
                    correlation: new_correlation,
                    subquery: Box::new(transform_ast(&related.subquery, t)),
                    system: related.system,
                    hidden: related.hidden,
                }),
                op: *op,
                flip: *flip,
                scalar: *scalar,
            }
        }
        Condition::And { conditions } => {
            let mapped: Vec<Condition> = conditions
                .iter()
                .map(|c| transform_where(c, table, t))
                .collect();
            Condition::And {
                conditions: (t.conditions_transform)(mapped),
            }
        }
        Condition::Or { conditions } => {
            let mapped: Vec<Condition> = conditions
                .iter()
                .map(|c| transform_where(c, table, t))
                .collect();
            Condition::Or {
                conditions: (t.conditions_transform)(mapped),
            }
        }
    }
}

/// TS `flattened(cond)`: collapse nested same-typed AND/OR, drop empties,
/// lift singletons.
pub fn flattened(cond: &Condition) -> Option<Condition> {
    match cond {
        Condition::Simple { .. } | Condition::CorrelatedSubquery { .. } => Some(cond.clone()),
        Condition::And { conditions } | Condition::Or { conditions } => {
            let is_and = matches!(cond, Condition::And { .. });
            let mut flat: Vec<Condition> = Vec::new();
            for c in conditions {
                let same_type = (is_and && matches!(c, Condition::And { .. }))
                    || (!is_and && matches!(c, Condition::Or { .. }));
                if same_type {
                    let inner = match c {
                        Condition::And { conditions } | Condition::Or { conditions } => conditions,
                        _ => unreachable!(),
                    };
                    for child in inner {
                        if let Some(f) = flattened(child) {
                            flat.push(f);
                        }
                    }
                } else if let Some(f) = flattened(c) {
                    flat.push(f);
                }
            }
            match flat.len() {
                0 => None,
                1 => Some(flat.into_iter().next().unwrap()),
                _ => {
                    if is_and {
                        Some(Condition::And { conditions: flat })
                    } else {
                        Some(Condition::Or { conditions: flat })
                    }
                }
            }
        }
    }
}

/// TS `compareUTF8`: byte-wise UTF-8 compare (same ordering as `&str`'s
/// `Ord` impl in Rust — both compare UTF-8 bytes).
fn compare_utf8(a: &str, b: &str) -> std::cmp::Ordering {
    a.cmp(b)
}

/// TS `cmpOptionalBool`: undefined < false < true.
fn cmp_optional_bool(a: Option<bool>, b: Option<bool>) -> std::cmp::Ordering {
    let to_num = |v: Option<bool>| match v {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    };
    to_num(a).cmp(&to_num(b))
}

/// TS `compareValuePosition`.
fn compare_value_position_lr(a: &ValuePosition, b: &ValuePosition) -> std::cmp::Ordering {
    let type_tag = |v: &ValuePosition| match v {
        ValuePosition::Literal { .. } => "literal",
        ValuePosition::Column { .. } => "column",
        ValuePosition::Static { .. } => "static",
    };
    let ta = type_tag(a);
    let tb = type_tag(b);
    if ta != tb {
        return compare_utf8(ta, tb);
    }
    match (a, b) {
        (ValuePosition::Literal { value: va }, ValuePosition::Literal { value: vb }) => {
            compare_utf8(&literal_to_string(va), &literal_to_string(vb))
        }
        (ValuePosition::Column { name: na }, ValuePosition::Column { name: nb }) => {
            compare_utf8(na, nb)
        }
        (ValuePosition::Static { .. }, ValuePosition::Static { .. }) => {
            panic!("Static parameters should be resolved before normalization");
        }
        _ => std::cmp::Ordering::Equal,
    }
}

/// Mirrors TS `compareValuePosition` for the non-column-allowed
/// right-hand side. `NonColumnValue::Static` is likewise an error at this
/// phase.
fn compare_non_column(a: &NonColumnValue, b: &NonColumnValue) -> std::cmp::Ordering {
    let type_tag = |v: &NonColumnValue| match v {
        NonColumnValue::Literal { .. } => "literal",
        NonColumnValue::Static { .. } => "static",
    };
    let ta = type_tag(a);
    let tb = type_tag(b);
    if ta != tb {
        return compare_utf8(ta, tb);
    }
    match (a, b) {
        (NonColumnValue::Literal { value: va }, NonColumnValue::Literal { value: vb }) => {
            compare_utf8(&literal_to_string(va), &literal_to_string(vb))
        }
        (NonColumnValue::Static { .. }, NonColumnValue::Static { .. }) => {
            panic!("Static parameters should be resolved before normalization");
        }
        _ => std::cmp::Ordering::Equal,
    }
}

/// TS `String(literalValue)` coercion for sort-key purposes. Matches JS
/// semantics for primitives and arrays (`[1,2].toString() === "1,2"`).
fn literal_to_string(v: &LiteralValue) -> String {
    match v {
        LiteralValue::String(s) => s.clone(),
        LiteralValue::Number(n) => {
            // JS `String(n)` for integer-valued f64 has no trailing `.0`.
            if n.fract() == 0.0 && n.is_finite() {
                format!("{}", *n as i64)
            } else {
                format!("{}", n)
            }
        }
        LiteralValue::Bool(b) => {
            if *b {
                "true".into()
            } else {
                "false".into()
            }
        }
        LiteralValue::Null => "null".into(),
        LiteralValue::Array(items) => items
            .iter()
            .map(|s| match s {
                ScalarLiteral::String(s) => s.clone(),
                ScalarLiteral::Number(n) => {
                    if n.fract() == 0.0 && n.is_finite() {
                        format!("{}", *n as i64)
                    } else {
                        format!("{}", n)
                    }
                }
                ScalarLiteral::Bool(b) => {
                    if *b {
                        "true".into()
                    } else {
                        "false".into()
                    }
                }
            })
            .collect::<Vec<_>>()
            .join(","),
    }
}

/// TS `cmpRelated`.
fn cmp_related(a: &CorrelatedSubquery, b: &CorrelatedSubquery) -> std::cmp::Ordering {
    let aa = a.subquery.alias.as_deref().unwrap_or_else(|| {
        panic!("CorrelatedSubquery missing alias in cmpRelated");
    });
    let bb = b.subquery.alias.as_deref().unwrap_or_else(|| {
        panic!("CorrelatedSubquery missing alias in cmpRelated");
    });
    compare_utf8(aa, bb)
}

/// TS `cmpCondition`.
fn cmp_condition(a: &Condition, b: &Condition) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    // Simple conditions sort first.
    if let Condition::Simple {
        left: la,
        right: ra,
        op: oa,
    } = a
    {
        if let Condition::Simple {
            left: lb,
            right: rb,
            op: ob,
        } = b
        {
            let c = compare_value_position_lr(la, lb);
            if c != Equal {
                return c;
            }
            // TS: `compareUTF8MaybeNull(a.op, b.op)` — op is the simple
            // operator token string ("=", "!=", …).
            let c = compare_simple_op(oa, ob);
            if c != Equal {
                return c;
            }
            return compare_non_column(ra, rb);
        }
        return Less;
    }
    if matches!(b, Condition::Simple { .. }) {
        return Greater;
    }

    if let Condition::CorrelatedSubquery {
        related: ra,
        op: oa,
        flip: fa,
        scalar: sa,
    } = a
    {
        if let Condition::CorrelatedSubquery {
            related: rb,
            op: ob,
            flip: fb,
            scalar: sb,
        } = b
        {
            let c = cmp_related(ra, rb);
            if c != Equal {
                return c;
            }
            let c = compare_utf8(
                match oa {
                    CorrelatedSubqueryOp::EXISTS => "EXISTS",
                    CorrelatedSubqueryOp::NotExists => "NOT EXISTS",
                },
                match ob {
                    CorrelatedSubqueryOp::EXISTS => "EXISTS",
                    CorrelatedSubqueryOp::NotExists => "NOT EXISTS",
                },
            );
            if c != Equal {
                return c;
            }
            let c = cmp_optional_bool(*fa, *fb);
            if c != Equal {
                return c;
            }
            return cmp_optional_bool(*sa, *sb);
        }
        return Less;
    }
    if matches!(b, Condition::CorrelatedSubquery { .. }) {
        return Greater;
    }

    // Both AND or OR.
    let (ta, ca) = match a {
        Condition::And { conditions } => ("and", conditions),
        Condition::Or { conditions } => ("or", conditions),
        _ => unreachable!(),
    };
    let (tb, cb) = match b {
        Condition::And { conditions } => ("and", conditions),
        Condition::Or { conditions } => ("or", conditions),
        _ => unreachable!(),
    };
    let c = compare_utf8(ta, tb);
    if c != Equal {
        return c;
    }
    let n = ca.len().min(cb.len());
    for i in 0..n {
        let c = cmp_condition(&ca[i], &cb[i]);
        if c != Equal {
            return c;
        }
    }
    ca.len().cmp(&cb.len())
}

fn compare_simple_op(a: &SimpleOperator, b: &SimpleOperator) -> std::cmp::Ordering {
    let s = |o: &SimpleOperator| match o {
        SimpleOperator::Eq => "=",
        SimpleOperator::Ne => "!=",
        SimpleOperator::IS => "IS",
        SimpleOperator::IsNot => "IS NOT",
        SimpleOperator::Lt => "<",
        SimpleOperator::Gt => ">",
        SimpleOperator::Le => "<=",
        SimpleOperator::Ge => ">=",
        SimpleOperator::LIKE => "LIKE",
        SimpleOperator::NotLike => "NOT LIKE",
        SimpleOperator::ILIKE => "ILIKE",
        SimpleOperator::NotIlike => "NOT ILIKE",
        SimpleOperator::IN => "IN",
        SimpleOperator::NotIn => "NOT IN",
    };
    compare_utf8(s(a), s(b))
}

/// Stable sort `related` subqueries by alias (TS `sortedRelated`).
fn sorted_related(mut r: Vec<CorrelatedSubquery>) -> Vec<CorrelatedSubquery> {
    r.sort_by(cmp_related);
    r
}

fn sorted_conditions(mut c: Vec<Condition>) -> Vec<Condition> {
    c.sort_by(cmp_condition);
    c
}

/// TS `normalizeAST(ast)`. Produces a canonical AST: conditions sorted,
/// where flattened, related sorted by alias, fields placed in canonical
/// order (by virtue of the Rust struct layout + serde).
pub fn normalize_ast(ast: &AST) -> AST {
    let mapper = IdentityNameMapper;
    let t = AstTransform {
        mapper: &mapper,
        related_transform: sorted_related,
        where_transform: flattened,
        conditions_transform: sorted_conditions,
    };
    transform_ast(ast, &t)
}

/// TS `mapAST(ast, mapper)`: name-remap pass (client↔server) with no
/// sorting / flattening.
pub fn map_ast(ast: &AST, mapper: &dyn NameMapper) -> AST {
    let t = AstTransform {
        mapper,
        related_transform: |r| r,
        where_transform: |w| Some(w.clone()),
        conditions_transform: |c| c,
    };
    transform_ast(ast, &t)
}

/// TS `mapCondition(cond, table, mapper)`.
pub fn map_condition(cond: &Condition, table: &str, mapper: &dyn NameMapper) -> Condition {
    let t = AstTransform {
        mapper,
        related_transform: |r| r,
        where_transform: |w| Some(w.clone()),
        conditions_transform: |c| c,
    };
    transform_where(cond, table, &t)
}

/// TS `h64(s)`: pack two `xxHash32` runs (seeds 0 and 1) into a 64-bit
/// integer: `(xxh32(s, 0) << 32) + xxh32(s, 1)`.
pub fn h64(s: &str) -> u64 {
    use xxhash_rust::xxh32::xxh32;
    let a = xxh32(s.as_bytes(), 0) as u64;
    let b = xxh32(s.as_bytes(), 1) as u64;
    (a << 32) + b
}

/// Lowercase base-36 encoding, matching JS `BigInt::toString(36)`.
pub fn to_base36(mut n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = Vec::with_capacity(13);
    while n > 0 {
        buf.push(ALPHABET[(n % 36) as usize]);
        n /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).unwrap()
}

/// TS `hashOfAST(ast)`. Byte-parity with TS:
/// `h64(JSON.stringify(normalizeAST(ast))).toString(36)`.
///
/// NOTE: the TS serializer is `JSON.stringify` (standard ECMA spec order,
/// no extra whitespace). Rust `serde_json::to_string(&T)` with struct
/// field order matching TS field order produces the same bytes for our
/// AST shape — we intentionally keep the field order aligned and tested.
pub fn hash_of_ast(ast: &AST) -> String {
    let normalized = normalize_ast(ast);
    let json = serde_json::to_string(&normalized).unwrap_or_default();
    to_base36(h64(&json))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_ast_round_trip() {
        let json = r#"{"table":"user","where":{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":"abc"}}}"#;
        let ast: AST = serde_json::from_str(json).unwrap();

        assert_eq!(ast.table, "user");
        assert!(matches!(
            ast.where_clause.as_deref(),
            Some(Condition::Simple { .. })
        ));

        let re_json = serde_json::to_string(&ast).unwrap();
        let re_ast: AST = serde_json::from_str(&re_json).unwrap();
        assert_eq!(ast, re_ast);
    }

    #[test]
    fn ast_with_ordering() {
        let json = r#"{"table":"issue","orderBy":[["created","desc"],["id","asc"]],"limit":50}"#;
        let ast: AST = serde_json::from_str(json).unwrap();

        assert_eq!(ast.limit, Some(50));
        let order = ast.order_by.as_ref().unwrap();
        assert_eq!(order.len(), 2);
        assert_eq!(order[0], ("created".into(), Direction::Desc));
        assert_eq!(order[1], ("id".into(), Direction::Asc));
    }

    #[test]
    fn nested_conditions() {
        let json = r#"{
            "table": "issue",
            "where": {
                "type": "and",
                "conditions": [
                    {"type": "simple", "op": "=", "left": {"type": "column", "name": "status"}, "right": {"type": "literal", "value": "open"}},
                    {"type": "or", "conditions": [
                        {"type": "simple", "op": ">", "left": {"type": "column", "name": "priority"}, "right": {"type": "literal", "value": 5}},
                        {"type": "simple", "op": "IS", "left": {"type": "column", "name": "assignee"}, "right": {"type": "literal", "value": null}}
                    ]}
                ]
            }
        }"#;

        let ast: AST = serde_json::from_str(json).unwrap();
        match ast.where_clause.as_deref() {
            Some(Condition::And { conditions }) => {
                assert_eq!(conditions.len(), 2);
                assert!(
                    matches!(&conditions[1], Condition::Or { conditions } if conditions.len() == 2)
                );
            }
            other => panic!("Expected And condition, got {other:?}"),
        }
    }

    #[test]
    fn correlated_subquery() {
        let json = r#"{
            "table": "issue",
            "related": [{
                "correlation": {"parentField": ["id"], "childField": ["issueId"]},
                "subquery": {"table": "comment"},
                "hidden": false
            }]
        }"#;

        let ast: AST = serde_json::from_str(json).unwrap();
        let related = ast.related.as_ref().unwrap();
        assert_eq!(related.len(), 1);
        assert_eq!(related[0].subquery.table, "comment");
        assert_eq!(related[0].correlation.parent_field, vec!["id"]);
    }

    #[test]
    fn operator_serde() {
        assert_eq!(
            serde_json::to_string(&SimpleOperator::Eq).unwrap(),
            r#""=""#
        );
        assert_eq!(
            serde_json::to_string(&SimpleOperator::IsNot).unwrap(),
            r#""IS NOT""#
        );
        assert_eq!(
            serde_json::to_string(&SimpleOperator::NotIn).unwrap(),
            r#""NOT IN""#
        );

        let op: SimpleOperator = serde_json::from_str(r#""<=""#).unwrap();
        assert_eq!(op, SimpleOperator::Le);
    }

    #[test]
    fn static_parameter() {
        let json = r#"{"type":"static","anchor":"authData","field":"sub"}"#;
        let vp: ValuePosition = serde_json::from_str(json).unwrap();
        match vp {
            ValuePosition::Static { anchor, field } => {
                assert_eq!(anchor, ParameterAnchor::AuthData);
                assert_eq!(field, ParameterField::Single("sub".into()));
            }
            other => panic!("Expected Static, got {other:?}"),
        }
    }

    #[test]
    fn static_parameter_path() {
        let json = r#"{"type":"static","anchor":"authData","field":["roles","admin"]}"#;
        let vp: ValuePosition = serde_json::from_str(json).unwrap();
        match vp {
            ValuePosition::Static { field, .. } => {
                assert_eq!(
                    field,
                    ParameterField::Path(vec!["roles".into(), "admin".into()])
                );
            }
            other => panic!("Expected Static with path, got {other:?}"),
        }
    }

    #[test]
    fn bind_static_simple() {
        // AST: SELECT * FROM conversations WHERE user_id = authData.sub
        let json = r#"{
            "table": "conversations",
            "where": {
                "type": "simple",
                "op": "=",
                "left": {"type": "column", "name": "user_id"},
                "right": {"type": "static", "anchor": "authData", "field": "sub"}
            }
        }"#;
        let ast: AST = serde_json::from_str(json).unwrap();

        let params = StaticQueryParameters {
            auth_data: serde_json::json!({"sub": "user123", "email": "test@example.com"}),
            pre_mutation_row: None,
        };

        let bound = bind_static_parameters(&ast, &params);

        // The static parameter should be replaced with a literal
        match bound.where_clause.as_deref() {
            Some(Condition::Simple { left, right, op }) => {
                assert_eq!(*op, SimpleOperator::Eq);
                assert!(matches!(left, ValuePosition::Column { name } if name == "user_id"));
                match right {
                    NonColumnValue::Literal { value } => {
                        assert_eq!(*value, LiteralValue::String("user123".into()));
                    }
                    other => panic!("Expected literal, got {other:?}"),
                }
            }
            other => panic!("Expected Simple condition, got {other:?}"),
        }
    }

    #[test]
    fn bind_static_nested_path() {
        // authData.profile.org_id
        let json = r#"{
            "table": "orgs",
            "where": {
                "type": "simple",
                "op": "=",
                "left": {"type": "column", "name": "id"},
                "right": {"type": "static", "anchor": "authData", "field": ["profile", "org_id"]}
            }
        }"#;
        let ast: AST = serde_json::from_str(json).unwrap();

        let params = StaticQueryParameters {
            auth_data: serde_json::json!({"profile": {"org_id": "org-456"}}),
            pre_mutation_row: None,
        };

        let bound = bind_static_parameters(&ast, &params);

        match bound.where_clause.as_deref() {
            Some(Condition::Simple { right, .. }) => match right {
                NonColumnValue::Literal { value } => {
                    assert_eq!(*value, LiteralValue::String("org-456".into()));
                }
                other => panic!("Expected literal, got {other:?}"),
            },
            other => panic!("Expected Simple, got {other:?}"),
        }
    }

    #[test]
    fn bind_static_missing_field_resolves_null() {
        let json = r#"{
            "table": "t",
            "where": {
                "type": "simple",
                "op": "=",
                "left": {"type": "column", "name": "id"},
                "right": {"type": "static", "anchor": "authData", "field": "nonexistent"}
            }
        }"#;
        let ast: AST = serde_json::from_str(json).unwrap();

        let params = StaticQueryParameters {
            auth_data: serde_json::json!({"sub": "user123"}),
            pre_mutation_row: None,
        };

        let bound = bind_static_parameters(&ast, &params);

        match bound.where_clause.as_deref() {
            Some(Condition::Simple { right, .. }) => match right {
                NonColumnValue::Literal { value } => {
                    assert_eq!(*value, LiteralValue::Null);
                }
                other => panic!("Expected literal null, got {other:?}"),
            },
            other => panic!("Expected Simple, got {other:?}"),
        }
    }

    #[test]
    fn bind_static_preserves_literals() {
        // Literal values should pass through unchanged
        let json = r#"{
            "table": "t",
            "where": {
                "type": "simple",
                "op": "=",
                "left": {"type": "column", "name": "status"},
                "right": {"type": "literal", "value": "active"}
            }
        }"#;
        let ast: AST = serde_json::from_str(json).unwrap();

        let params = StaticQueryParameters {
            auth_data: serde_json::json!({}),
            pre_mutation_row: None,
        };

        let bound = bind_static_parameters(&ast, &params);

        // Should be unchanged
        assert_eq!(ast, bound);
    }

    // ---------------------------------------------------------------------
    // Tests ported from zero-protocol/src/ast.test.ts that require functions
    // not yet implemented in Rust (`normalize_ast`, `map_ast`, `ast_schema`).
    // Marked #[ignore] so they light up once the implementation catches up.
    // ---------------------------------------------------------------------

    #[test]
    fn fields_are_placed_into_correct_positions() {
        // TS test: `fields are placed into correct positions` (ast.test.ts).
        // Verifies that normalizeAST returns the same JSON regardless of
        // the order in which fields were specified on the input.
        let a: AST = serde_json::from_str(
            r#"{"table":"foo","orderBy":[["id","asc"]],"limit":10,"alias":"x"}"#,
        )
        .unwrap();
        let b: AST = serde_json::from_str(
            r#"{"alias":"x","limit":10,"orderBy":[["id","asc"]],"table":"foo"}"#,
        )
        .unwrap();
        let na = normalize_ast(&a);
        let nb = normalize_ast(&b);
        assert_eq!(
            serde_json::to_string(&na).unwrap(),
            serde_json::to_string(&nb).unwrap()
        );
    }

    #[test]
    fn conditions_are_sorted() {
        // TS test: `conditions are sorted`.
        // normalizeAST must sort `and`/`or` conditions deterministically.
        let ast: AST = serde_json::from_str(
            r#"{
                "table": "t",
                "where": {
                    "type": "and",
                    "conditions": [
                        {"type":"simple","op":"=","left":{"type":"column","name":"z"},"right":{"type":"literal","value":1}},
                        {"type":"simple","op":"=","left":{"type":"column","name":"a"},"right":{"type":"literal","value":2}},
                        {"type":"simple","op":"=","left":{"type":"column","name":"m"},"right":{"type":"literal","value":3}}
                    ]
                }
            }"#,
        )
        .unwrap();
        let n = normalize_ast(&ast);
        match n.where_clause.as_deref() {
            Some(Condition::And { conditions }) => {
                let names: Vec<_> = conditions
                    .iter()
                    .filter_map(|c| match c {
                        Condition::Simple {
                            left: ValuePosition::Column { name },
                            ..
                        } => Some(name.as_str()),
                        _ => None,
                    })
                    .collect();
                assert_eq!(names, vec!["a", "m", "z"]);
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn related_subqueries_are_sorted() {
        // TS test: `related subqueries are sorted`.
        let ast: AST = serde_json::from_str(
            r#"{
                "table": "t",
                "related": [
                    {"correlation":{"parentField":["id"],"childField":["tid"]},"subquery":{"table":"c","alias":"zz"}},
                    {"correlation":{"parentField":["id"],"childField":["tid"]},"subquery":{"table":"c","alias":"aa"}},
                    {"correlation":{"parentField":["id"],"childField":["tid"]},"subquery":{"table":"c","alias":"mm"}}
                ]
            }"#,
        )
        .unwrap();
        let n = normalize_ast(&ast);
        let aliases: Vec<_> = n
            .related
            .unwrap()
            .into_iter()
            .map(|r| r.subquery.alias.unwrap())
            .collect();
        assert_eq!(aliases, vec!["aa", "mm", "zz"]);
    }

    #[test]
    fn make_server_ast() {
        // TS test: `makeServerAST` (ast.test.ts). Uses a mapper that prefixes
        // table/column names with `server_`.
        struct PrefixMapper;
        impl NameMapper for PrefixMapper {
            fn table_name(&self, t: &str) -> String {
                format!("srv_{}", t)
            }
            fn column_name(&self, _t: &str, c: &str) -> String {
                format!("col_{}", c)
            }
        }
        let ast: AST = serde_json::from_str(
            r#"{
                "table": "users",
                "where": {"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1}},
                "orderBy": [["id","asc"]],
                "related": [
                    {"correlation":{"parentField":["id"],"childField":["uid"]},"subquery":{"table":"posts","alias":"p"}}
                ]
            }"#,
        )
        .unwrap();

        let server = map_ast(&ast, &PrefixMapper);
        assert_eq!(server.table, "srv_users");
        // orderBy column remapped
        assert_eq!(server.order_by.as_ref().unwrap()[0].0, "col_id");
        // where column remapped
        match server.where_clause.as_deref() {
            Some(Condition::Simple {
                left: ValuePosition::Column { name },
                ..
            }) => assert_eq!(name, "col_id"),
            other => panic!("expected simple, got {other:?}"),
        }
        // related subquery table remapped, correlation columns remapped
        let r = &server.related.unwrap()[0];
        assert_eq!(r.subquery.table, "srv_posts");
        assert_eq!(r.correlation.parent_field, vec!["col_id".to_string()]);
        assert_eq!(r.correlation.child_field, vec!["col_uid".to_string()]);
    }

    #[test]
    #[ignore = "needs TS `astSchema` (valita schema for AST) ported from \
                packages/zero-protocol/src/ast.ts — tracked in \
                docs/ts-vs-rs-comparison.md row 1 (`Protocol & types`)"]
    fn protocol_version_hash() {
        // TS test: `protocol version`. Hashes the AST zod schema and asserts
        // both the hash and PROTOCOL_VERSION constant haven't changed.
    }

    #[test]
    fn bind_static_nested_subquery_pre_mutation_row() {
        // Ported from zql/src/builder/builder.test.ts `bind static parameters`.
        // A nested correlated subquery references `preMutationRow.stateCode`.
        let json = r#"{
            "table": "users",
            "orderBy": [["id", "asc"]],
            "where": {
                "type": "simple",
                "op": "=",
                "left": {"type": "column", "name": "id"},
                "right": {"type": "static", "anchor": "authData", "field": "userID"}
            },
            "related": [
                {
                    "system": "client",
                    "correlation": {"parentField": ["id"], "childField": ["userID"]},
                    "subquery": {
                        "table": "userStates",
                        "alias": "userStates",
                        "where": {
                            "type": "simple",
                            "op": "=",
                            "left": {"type": "column", "name": "stateCode"},
                            "right": {"type": "static", "anchor": "preMutationRow", "field": "stateCode"}
                        }
                    }
                }
            ]
        }"#;
        let ast: AST = serde_json::from_str(json).unwrap();

        let params = StaticQueryParameters {
            auth_data: serde_json::json!({"userID": 1}),
            pre_mutation_row: Some(serde_json::json!({"stateCode": "HI"})),
        };

        let bound = bind_static_parameters(&ast, &params);

        // Outer where: id = 1 (resolved from authData.userID)
        match bound.where_clause.as_deref() {
            Some(Condition::Simple { right, .. }) => match right {
                NonColumnValue::Literal { value } => {
                    assert_eq!(*value, LiteralValue::Number(1.0));
                }
                other => panic!("Expected literal number, got {other:?}"),
            },
            other => panic!("Expected Simple, got {other:?}"),
        }

        // Nested subquery where: stateCode = "HI" (from preMutationRow)
        let related = bound.related.as_ref().unwrap();
        assert_eq!(related.len(), 1);
        let sub = &related[0].subquery;
        match sub.where_clause.as_deref() {
            Some(Condition::Simple { right, .. }) => match right {
                NonColumnValue::Literal { value } => {
                    assert_eq!(*value, LiteralValue::String("HI".into()));
                }
                other => panic!("Expected literal HI, got {other:?}"),
            },
            other => panic!("Expected Simple in sub, got {other:?}"),
        }
    }

    #[test]
    fn bind_static_and_conditions() {
        let json = r#"{
            "table": "t",
            "where": {
                "type": "and",
                "conditions": [
                    {"type": "simple", "op": "=", "left": {"type": "column", "name": "org"}, "right": {"type": "static", "anchor": "authData", "field": "org_id"}},
                    {"type": "simple", "op": "=", "left": {"type": "column", "name": "active"}, "right": {"type": "literal", "value": true}}
                ]
            }
        }"#;
        let ast: AST = serde_json::from_str(json).unwrap();

        let params = StaticQueryParameters {
            auth_data: serde_json::json!({"org_id": "org-1"}),
            pre_mutation_row: None,
        };

        let bound = bind_static_parameters(&ast, &params);

        match bound.where_clause.as_deref() {
            Some(Condition::And { conditions }) => {
                assert_eq!(conditions.len(), 2);
                // First: org = "org-1" (was static, now literal)
                match &conditions[0] {
                    Condition::Simple { right, .. } => match right {
                        NonColumnValue::Literal { value } => {
                            assert_eq!(*value, LiteralValue::String("org-1".into()));
                        }
                        other => panic!("Expected literal, got {other:?}"),
                    },
                    other => panic!("Expected Simple, got {other:?}"),
                }
                // Second: active = true (was literal, still literal)
                match &conditions[1] {
                    Condition::Simple { right, .. } => match right {
                        NonColumnValue::Literal { value } => {
                            assert_eq!(*value, LiteralValue::Bool(true));
                        }
                        other => panic!("Expected literal, got {other:?}"),
                    },
                    other => panic!("Expected Simple, got {other:?}"),
                }
            }
            other => panic!("Expected And, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // h64 / hash_of_ast byte-parity test vectors.
    //
    // Reference values computed in Node via
    //   `const {xxHash32} = require('js-xxhash');
    //    h64(s) = ((BigInt(xxHash32(s,0)) << 32n) + BigInt(xxHash32(s,1)))
    //    h64(s).toString(36)`
    // -----------------------------------------------------------------

    #[test]
    fn h64_empty_string_parity() {
        assert_eq!(to_base36(h64("")), "1j5etzlexqqa");
    }

    #[test]
    fn h64_abc_parity() {
        assert_eq!(to_base36(h64("abc")), "rtjjvs99c9of");
    }

    #[test]
    fn h64_hello_world_parity() {
        assert_eq!(to_base36(h64("hello world")), "356e0ewompmb0");
    }

    #[test]
    fn h64_ast_json_parity() {
        // Hash of the JSON bytes `{"table":"t"}` — the normalised serialized
        // form of an AST that has only `table: "t"` set. Reference value
        // computed in Node. This is the byte-parity gate against TS.
        assert_eq!(to_base36(h64(r#"{"table":"t"}"#)), "m3t2gy932pnc");
    }

    #[test]
    fn h64_ast_with_where_parity() {
        let s = r#"{"table":"t","where":{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1}}}"#;
        assert_eq!(to_base36(h64(s)), "3kl2drewpxq0j");
    }

    #[test]
    fn hash_of_ast_minimal_matches_h64_of_normalized_json() {
        let ast: AST = serde_json::from_str(r#"{"table":"t"}"#).unwrap();
        // hash_of_ast must equal to_base36(h64(JSON.stringify(normalize_ast))).
        let normalized = normalize_ast(&ast);
        let json = serde_json::to_string(&normalized).unwrap();
        assert_eq!(hash_of_ast(&ast), to_base36(h64(&json)));
    }

    #[test]
    fn hash_of_ast_is_stable_across_field_order() {
        let a: AST =
            serde_json::from_str(r#"{"table":"foo","orderBy":[["id","asc"]],"limit":10}"#).unwrap();
        let b: AST =
            serde_json::from_str(r#"{"limit":10,"orderBy":[["id","asc"]],"table":"foo"}"#).unwrap();
        assert_eq!(hash_of_ast(&a), hash_of_ast(&b));
    }

    #[test]
    fn hash_of_ast_distinguishes_different_tables() {
        let a: AST = serde_json::from_str(r#"{"table":"a"}"#).unwrap();
        let b: AST = serde_json::from_str(r#"{"table":"b"}"#).unwrap();
        assert_ne!(hash_of_ast(&a), hash_of_ast(&b));
    }

    #[test]
    fn flattened_collapses_singletons() {
        let cond = Condition::And {
            conditions: vec![Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column { name: "x".into() },
                right: NonColumnValue::Literal {
                    value: LiteralValue::Number(1.0),
                },
            }],
        };
        let f = flattened(&cond).unwrap();
        assert!(matches!(f, Condition::Simple { .. }));
    }

    #[test]
    fn flattened_merges_nested_and() {
        let inner = Condition::And {
            conditions: vec![Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column { name: "a".into() },
                right: NonColumnValue::Literal {
                    value: LiteralValue::Number(1.0),
                },
            }],
        };
        let outer = Condition::And {
            conditions: vec![
                Condition::Simple {
                    op: SimpleOperator::Eq,
                    left: ValuePosition::Column { name: "b".into() },
                    right: NonColumnValue::Literal {
                        value: LiteralValue::Number(2.0),
                    },
                },
                inner,
            ],
        };
        let f = flattened(&outer).unwrap();
        match f {
            Condition::And { conditions } => {
                assert_eq!(conditions.len(), 2);
                // Both simples preserved, nested AND with single child was lifted.
                for c in &conditions {
                    assert!(matches!(c, Condition::Simple { .. }));
                }
            }
            other => panic!("expected AND, got {other:?}"),
        }
    }

    #[test]
    fn flattened_drops_empty_and_or() {
        let cond = Condition::And { conditions: vec![] };
        assert!(flattened(&cond).is_none());
    }

    #[test]
    fn map_condition_renames_columns() {
        struct M;
        impl NameMapper for M {
            fn table_name(&self, t: &str) -> String {
                t.to_string()
            }
            fn column_name(&self, _t: &str, c: &str) -> String {
                c.to_uppercase()
            }
        }
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column { name: "id".into() },
            right: NonColumnValue::Literal {
                value: LiteralValue::Number(1.0),
            },
        };
        let out = map_condition(&cond, "t", &M);
        match out {
            Condition::Simple {
                left: ValuePosition::Column { name },
                ..
            } => assert_eq!(name, "ID"),
            other => panic!("unexpected: {other:?}"),
        }
    }
}
