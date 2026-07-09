//! Deserializes a [`JsonValue`] (as produced by `bigint_json::parse`, e.g.
//! from a `clientAST` JSONB column) into a real [`Ast`] — the counterpart
//! to `ast.ts`'s `astSchema` (a `valita` schema) this crate didn't have
//! until now. Closes the gap `cvr_store_pg::load_cvr` left open: parsing
//! `queryArgs` only needed the generic JSON parser, but `clientAST` needs
//! walking that JSON into the recursive, strongly-typed `Ast`/`Condition`/
//! `ValuePosition` structures — genuinely different, harder work.
//!
//! Field names below are the exact wire keys `astSchema` uses (camelCase
//! JSON, e.g. `orderBy`, `parentField`) — this is a real deserializer for
//! the wire format, not a Rust-convention mirror.

use zero_cache_shared::bigint_json::JsonValue;

use crate::ast::{
    Ast, Bound, ColumnReference, Condition, CorrelatedSubquery, Correlation, Direction, ExistsOp,
    LiteralValue, Ordering, Parameter, SimpleOperator, System, ValuePosition,
};

/// Error deserializing a `JsonValue` into an AST type. Port of the
/// validation errors `valita` would raise for a malformed `astSchema`
/// payload — collapsed into one variant with a path-ish message rather
/// than valita's structured issue list, since this crate has no schema
/// library to integrate with.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid AST JSON: {0}")]
pub struct AstJsonError(pub String);

fn err(msg: impl Into<String>) -> AstJsonError {
    AstJsonError(msg.into())
}

/// Serializes an [`Ast`] back to a [`JsonValue`] using the same wire keys
/// `astSchema`/`ast_from_json` use. The counterpart to `ast_from_json` —
/// needed by `query_hash::hash_of_ast` (`hashOfAST` calls `JSON.stringify`
/// on the normalized AST). Only emits fields that are present (`None` ->
/// omitted key, matching `JSON.stringify`'s own `undefined`-drops-the-key
/// behavior, not `null`).
pub fn ast_to_json(ast: &Ast) -> JsonValue {
    let mut fields: Vec<(String, JsonValue)> = Vec::new();
    if let Some(schema) = &ast.schema {
        fields.push(("schema".into(), JsonValue::String(schema.clone())));
    }
    fields.push(("table".into(), JsonValue::String(ast.table.clone())));
    if let Some(alias) = &ast.alias {
        fields.push(("alias".into(), JsonValue::String(alias.clone())));
    }
    if let Some(where_) = &ast.where_ {
        fields.push(("where".into(), condition_to_json(where_)));
    }
    if let Some(related) = &ast.related {
        fields.push((
            "related".into(),
            JsonValue::Array(related.iter().map(correlated_subquery_to_json).collect()),
        ));
    }
    if let Some(start) = &ast.start {
        fields.push((
            "start".into(),
            JsonValue::Object(vec![
                ("row".to_string(), start.row.clone()),
                ("exclusive".to_string(), JsonValue::Bool(start.exclusive)),
            ]),
        ));
    }
    if let Some(limit) = ast.limit {
        fields.push(("limit".into(), JsonValue::Number(limit)));
    }
    if let Some(order_by) = &ast.order_by {
        fields.push((
            "orderBy".into(),
            JsonValue::Array(
                order_by
                    .iter()
                    .map(|(field, dir)| {
                        JsonValue::Array(vec![
                            JsonValue::String(field.clone()),
                            JsonValue::String(dir.as_str().to_string()),
                        ])
                    })
                    .collect(),
            ),
        ));
    }
    JsonValue::Object(fields)
}

fn correlated_subquery_to_json(cs: &CorrelatedSubquery) -> JsonValue {
    let mut fields: Vec<(String, JsonValue)> = vec![
        (
            "correlation".to_string(),
            JsonValue::Object(vec![
                (
                    "parentField".to_string(),
                    JsonValue::Array(
                        cs.correlation
                            .parent_field
                            .iter()
                            .cloned()
                            .map(JsonValue::String)
                            .collect(),
                    ),
                ),
                (
                    "childField".to_string(),
                    JsonValue::Array(
                        cs.correlation
                            .child_field
                            .iter()
                            .cloned()
                            .map(JsonValue::String)
                            .collect(),
                    ),
                ),
            ]),
        ),
        ("subquery".to_string(), ast_to_json(&cs.subquery)),
    ];
    if let Some(system) = cs.system {
        fields.push((
            "system".to_string(),
            JsonValue::String(system_to_str(system).to_string()),
        ));
    }
    if let Some(hidden) = cs.hidden {
        fields.push(("hidden".to_string(), JsonValue::Bool(hidden)));
    }
    JsonValue::Object(fields)
}

fn system_to_str(system: System) -> &'static str {
    match system {
        System::Permissions => "permissions",
        System::Client => "client",
        System::Test => "test",
    }
}

fn literal_value_to_json(v: &LiteralValue) -> JsonValue {
    match v {
        LiteralValue::String(s) => JsonValue::String(s.clone()),
        LiteralValue::Number(n) => JsonValue::Number(*n),
        LiteralValue::Bool(b) => JsonValue::Bool(*b),
        LiteralValue::Null => JsonValue::Null,
        LiteralValue::Array(items) => {
            JsonValue::Array(items.iter().map(literal_value_to_json).collect())
        }
    }
}

fn value_position_to_json(v: &ValuePosition) -> JsonValue {
    match v {
        ValuePosition::Literal(lit) => JsonValue::Object(vec![
            ("type".to_string(), JsonValue::String("literal".to_string())),
            ("value".to_string(), literal_value_to_json(lit)),
        ]),
        ValuePosition::Column(c) => JsonValue::Object(vec![
            ("type".to_string(), JsonValue::String("column".to_string())),
            ("name".to_string(), JsonValue::String(c.name.clone())),
        ]),
        ValuePosition::Parameter(p) => p.raw.clone(),
    }
}

fn condition_to_json(cond: &Condition) -> JsonValue {
    match cond {
        Condition::Simple { op, left, right } => JsonValue::Object(vec![
            ("type".to_string(), JsonValue::String("simple".to_string())),
            ("op".to_string(), JsonValue::String(op.as_str().to_string())),
            ("left".to_string(), value_position_to_json(left)),
            ("right".to_string(), value_position_to_json(right)),
        ]),
        Condition::And { conditions } => JsonValue::Object(vec![
            ("type".to_string(), JsonValue::String("and".to_string())),
            (
                "conditions".to_string(),
                JsonValue::Array(conditions.iter().map(condition_to_json).collect()),
            ),
        ]),
        Condition::Or { conditions } => JsonValue::Object(vec![
            ("type".to_string(), JsonValue::String("or".to_string())),
            (
                "conditions".to_string(),
                JsonValue::Array(conditions.iter().map(condition_to_json).collect()),
            ),
        ]),
        Condition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
            plan_id: _,
        } => {
            let mut fields = vec![
                (
                    "type".to_string(),
                    JsonValue::String("correlatedSubquery".to_string()),
                ),
                ("related".to_string(), correlated_subquery_to_json(related)),
                ("op".to_string(), JsonValue::String(op.as_str().to_string())),
            ];
            if let Some(flip) = flip {
                fields.push(("flip".to_string(), JsonValue::Bool(*flip)));
            }
            if let Some(scalar) = scalar {
                fields.push(("scalar".to_string(), JsonValue::Bool(*scalar)));
            }
            JsonValue::Object(fields)
        }
    }
}

fn as_object(v: &JsonValue) -> Result<&Vec<(String, JsonValue)>, AstJsonError> {
    match v {
        JsonValue::Object(entries) => Ok(entries),
        other => Err(err(format!("expected object, got {other:?}"))),
    }
}

fn field<'a>(obj: &'a [(String, JsonValue)], key: &str) -> Option<&'a JsonValue> {
    obj.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

fn required<'a>(obj: &'a [(String, JsonValue)], key: &str) -> Result<&'a JsonValue, AstJsonError> {
    field(obj, key).ok_or_else(|| err(format!("missing required field {key:?}")))
}

fn as_str<'a>(v: &'a JsonValue) -> Result<&'a str, AstJsonError> {
    match v {
        JsonValue::String(s) => Ok(s.as_str()),
        other => Err(err(format!("expected string, got {other:?}"))),
    }
}

fn as_bool(v: &JsonValue) -> Result<bool, AstJsonError> {
    match v {
        JsonValue::Bool(b) => Ok(*b),
        other => Err(err(format!("expected boolean, got {other:?}"))),
    }
}

fn as_f64(v: &JsonValue) -> Result<f64, AstJsonError> {
    match v {
        JsonValue::Number(n) => Ok(*n),
        other => Err(err(format!("expected number, got {other:?}"))),
    }
}

fn as_array<'a>(v: &'a JsonValue) -> Result<&'a Vec<JsonValue>, AstJsonError> {
    match v {
        JsonValue::Array(items) => Ok(items),
        other => Err(err(format!("expected array, got {other:?}"))),
    }
}

fn string_at(obj: &[(String, JsonValue)], key: &str) -> Result<Option<String>, AstJsonError> {
    field(obj, key)
        .map(as_str)
        .transpose()
        .map(|o| o.map(str::to_string))
}

fn bool_at(obj: &[(String, JsonValue)], key: &str) -> Result<Option<bool>, AstJsonError> {
    field(obj, key).map(as_bool).transpose()
}

/// Port of `compoundKeySchema` (`[string, ...string[]]`).
fn compound_key_from_json(v: &JsonValue) -> Result<Vec<String>, AstJsonError> {
    let items = as_array(v)?;
    if items.is_empty() {
        return Err(err("compound key must be non-empty"));
    }
    items
        .iter()
        .map(as_str)
        .map(|r| r.map(str::to_string))
        .collect()
}

fn system_from_json(v: &JsonValue) -> Result<System, AstJsonError> {
    match as_str(v)? {
        "permissions" => Ok(System::Permissions),
        "client" => Ok(System::Client),
        "test" => Ok(System::Test),
        other => Err(err(format!("unknown system {other:?}"))),
    }
}

/// Port of `literalReferenceSchema`'s `value` field (scalar or scalar
/// array).
fn literal_value_from_json(v: &JsonValue) -> Result<LiteralValue, AstJsonError> {
    Ok(match v {
        JsonValue::Null => LiteralValue::Null,
        JsonValue::Bool(b) => LiteralValue::Bool(*b),
        JsonValue::Number(n) => LiteralValue::Number(*n),
        JsonValue::String(s) => LiteralValue::String(s.clone()),
        JsonValue::Array(items) => LiteralValue::Array(
            items
                .iter()
                .map(literal_value_from_json)
                .collect::<Result<_, _>>()?,
        ),
        other => return Err(err(format!("invalid literal value {other:?}"))),
    })
}

/// Port of `conditionValueSchema` (`literalReferenceSchema |
/// columnReferenceSchema | parameterReferenceSchema`), discriminated by the
/// `type` field.
fn value_position_from_json(v: &JsonValue) -> Result<ValuePosition, AstJsonError> {
    let obj = as_object(v)?;
    match as_str(required(obj, "type")?)? {
        "literal" => Ok(ValuePosition::Literal(literal_value_from_json(required(
            obj, "value",
        )?)?)),
        "column" => Ok(ValuePosition::Column(ColumnReference {
            name: as_str(required(obj, "name")?)?.to_string(),
        })),
        "static" => Ok(ValuePosition::Parameter(Parameter { raw: v.clone() })),
        other => Err(err(format!("unknown ValuePosition type {other:?}"))),
    }
}

fn exists_op_from_json(v: &JsonValue) -> Result<ExistsOp, AstJsonError> {
    ExistsOp::from_str(as_str(v)?).ok_or_else(|| err(format!("unknown exists op {v:?}")))
}

/// Port of `conditionSchema`, discriminated by `type`.
pub fn condition_from_json(v: &JsonValue) -> Result<Condition, AstJsonError> {
    let obj = as_object(v)?;
    match as_str(required(obj, "type")?)? {
        "simple" => {
            let op = SimpleOperator::from_str(as_str(required(obj, "op")?)?)
                .ok_or_else(|| err(format!("unknown simple operator {:?}", required(obj, "op"))))?;
            Ok(Condition::Simple {
                op,
                left: value_position_from_json(required(obj, "left")?)?,
                right: value_position_from_json(required(obj, "right")?)?,
            })
        }
        "and" => Ok(Condition::And {
            conditions: as_array(required(obj, "conditions")?)?
                .iter()
                .map(condition_from_json)
                .collect::<Result<_, _>>()?,
        }),
        "or" => Ok(Condition::Or {
            conditions: as_array(required(obj, "conditions")?)?
                .iter()
                .map(condition_from_json)
                .collect::<Result<_, _>>()?,
        }),
        "correlatedSubquery" => Ok(Condition::CorrelatedSubquery {
            related: correlated_subquery_from_json(required(obj, "related")?)?,
            op: exists_op_from_json(required(obj, "op")?)?,
            flip: bool_at(obj, "flip")?,
            scalar: bool_at(obj, "scalar")?,
            // `plan_id` is never part of the wire AST shape (see the field's
            // doc comment on `Condition::CorrelatedSubquery` in `ast.rs`).
            plan_id: None,
        }),
        other => Err(err(format!("unknown condition type {other:?}"))),
    }
}

/// Port of `correlatedSubquerySchema`.
pub fn correlated_subquery_from_json(v: &JsonValue) -> Result<CorrelatedSubquery, AstJsonError> {
    let obj = as_object(v)?;
    let correlation_obj = as_object(required(obj, "correlation")?)?;
    Ok(CorrelatedSubquery {
        correlation: Correlation {
            parent_field: compound_key_from_json(required(correlation_obj, "parentField")?)?,
            child_field: compound_key_from_json(required(correlation_obj, "childField")?)?,
        },
        subquery: Box::new(ast_from_json(required(obj, "subquery")?)?),
        system: field(obj, "system").map(system_from_json).transpose()?,
        hidden: bool_at(obj, "hidden")?,
    })
}

/// Port of `astSchema.start` (`{row, exclusive}`).
fn bound_from_json(v: &JsonValue) -> Result<Bound, AstJsonError> {
    let obj = as_object(v)?;
    Ok(Bound {
        row: required(obj, "row")?.clone(),
        exclusive: as_bool(required(obj, "exclusive")?)?,
    })
}

/// Port of `orderingSchema` (`[[field, 'asc'|'desc'], ...]`).
fn ordering_from_json(v: &JsonValue) -> Result<Ordering, AstJsonError> {
    as_array(v)?
        .iter()
        .map(|el| {
            let pair = as_array(el)?;
            if pair.len() != 2 {
                return Err(err("ordering element must be [field, direction]"));
            }
            let field = as_str(&pair[0])?.to_string();
            let direction = Direction::from_str(as_str(&pair[1])?)
                .ok_or_else(|| err(format!("unknown direction {:?}", pair[1])))?;
            Ok((field, direction))
        })
        .collect()
}

/// Port of `astSchema`. The main entry point of this module.
pub fn ast_from_json(v: &JsonValue) -> Result<Ast, AstJsonError> {
    let obj = as_object(v)?;
    Ok(Ast {
        schema: string_at(obj, "schema")?,
        table: as_str(required(obj, "table")?)?.to_string(),
        alias: string_at(obj, "alias")?,
        where_: field(obj, "where").map(condition_from_json).transpose()?,
        related: field(obj, "related")
            .map(|v| {
                as_array(v)?
                    .iter()
                    .map(correlated_subquery_from_json)
                    .collect::<Result<_, _>>()
            })
            .transpose()?,
        start: field(obj, "start").map(bound_from_json).transpose()?,
        limit: field(obj, "limit").map(as_f64).transpose()?,
        order_by: field(obj, "orderBy").map(ordering_from_json).transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_shared::bigint_json::parse as parse_json;

    fn ast_json(s: &str) -> Ast {
        ast_from_json(&parse_json(s).unwrap()).unwrap()
    }

    #[test]
    fn minimal_ast_just_table() {
        let ast = ast_json(r#"{"table":"issues"}"#);
        assert_eq!(
            ast,
            Ast {
                table: "issues".into(),
                ..Default::default()
            }
        );
    }

    #[test]
    fn ast_with_schema_and_alias() {
        let ast = ast_json(r#"{"table":"issues","schema":"public","alias":"i"}"#);
        assert_eq!(ast.schema, Some("public".to_string()));
        assert_eq!(ast.alias, Some("i".to_string()));
    }

    #[test]
    fn simple_where_condition() {
        let ast = ast_json(
            r#"{"table":"issues","where":{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1}}}"#,
        );
        assert_eq!(
            ast.where_,
            Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference { name: "id".into() }),
                right: ValuePosition::Literal(LiteralValue::Number(1.0)),
            })
        );
    }

    #[test]
    fn and_or_conditions_are_recursive() {
        let ast = ast_json(
            r#"{"table":"t","where":{"type":"and","conditions":[
                {"type":"simple","op":"=","left":{"type":"column","name":"a"},"right":{"type":"literal","value":1}},
                {"type":"or","conditions":[
                    {"type":"simple","op":"=","left":{"type":"column","name":"b"},"right":{"type":"literal","value":2}}
                ]}
            ]}}"#,
        );
        let Some(Condition::And { conditions }) = &ast.where_ else {
            panic!("expected And")
        };
        assert_eq!(conditions.len(), 2);
        assert!(matches!(conditions[1], Condition::Or { .. }));
    }

    #[test]
    fn literal_array_value() {
        let ast = ast_json(
            r#"{"table":"t","where":{"type":"simple","op":"IN","left":{"type":"column","name":"a"},"right":{"type":"literal","value":[1,2,3]}}}"#,
        );
        let Some(Condition::Simple { right, .. }) = &ast.where_ else {
            panic!()
        };
        assert_eq!(
            right,
            &ValuePosition::Literal(LiteralValue::Array(vec![
                LiteralValue::Number(1.0),
                LiteralValue::Number(2.0),
                LiteralValue::Number(3.0),
            ]))
        );
    }

    #[test]
    fn static_parameter_is_kept_opaque() {
        let ast = ast_json(
            r#"{"table":"t","where":{"type":"simple","op":"=","left":{"type":"column","name":"a"},"right":{"type":"static","anchor":"authData","field":"sub"}}}"#,
        );
        let Some(Condition::Simple {
            right: ValuePosition::Parameter(p),
            ..
        }) = &ast.where_
        else {
            panic!("expected Parameter")
        };
        assert!(matches!(&p.raw, JsonValue::Object(_)));
    }

    #[test]
    fn correlated_subquery_condition_recurses_into_subquery() {
        let ast = ast_json(
            r#"{"table":"issues","where":{
                "type":"correlatedSubquery",
                "op":"EXISTS",
                "related":{
                    "correlation":{"parentField":["id"],"childField":["issueID"]},
                    "subquery":{"table":"comments"}
                }
            }}"#,
        );
        let Some(Condition::CorrelatedSubquery { related, op, .. }) = &ast.where_ else {
            panic!("expected CorrelatedSubquery")
        };
        assert_eq!(op, &ExistsOp::Exists);
        assert_eq!(related.subquery.table, "comments");
        assert_eq!(related.correlation.parent_field, vec!["id".to_string()]);
    }

    #[test]
    fn order_by_and_limit_and_start() {
        let ast = ast_json(
            r#"{"table":"t","orderBy":[["id","asc"],["name","desc"]],"limit":10,"start":{"row":{"id":1},"exclusive":true}}"#,
        );
        assert_eq!(
            ast.order_by,
            Some(vec![
                ("id".to_string(), Direction::Asc),
                ("name".to_string(), Direction::Desc)
            ])
        );
        assert_eq!(ast.limit, Some(10.0));
        assert_eq!(ast.start.unwrap().exclusive, true);
    }

    #[test]
    fn related_subqueries() {
        let ast = ast_json(
            r#"{"table":"issues","related":[{
                "correlation":{"parentField":["id"],"childField":["issueID"]},
                "subquery":{"table":"comments"},
                "system":"client",
                "hidden":false
            }]}"#,
        );
        let related = ast.related.unwrap();
        assert_eq!(related.len(), 1);
        assert_eq!(related[0].system, Some(System::Client));
        assert_eq!(related[0].hidden, Some(false));
    }

    #[test]
    fn missing_required_field_errors() {
        assert!(ast_from_json(&parse_json(r#"{}"#).unwrap()).is_err());
    }

    #[test]
    fn unknown_condition_type_errors() {
        let result =
            ast_from_json(&parse_json(r#"{"table":"t","where":{"type":"bogus"}}"#).unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn unknown_simple_operator_errors() {
        let result = ast_from_json(
            &parse_json(
                r#"{"table":"t","where":{"type":"simple","op":"WEIRD","left":{"type":"column","name":"a"},"right":{"type":"literal","value":1}}}"#,
            )
            .unwrap(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn ast_to_json_round_trips_through_ast_from_json() {
        let ast = ast_json(
            r#"{"table":"issue","schema":"public","alias":"i",
               "where":{"type":"and","conditions":[
                 {"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1}},
                 {"type":"correlatedSubquery","op":"EXISTS","flip":true,
                  "related":{"correlation":{"parentField":["id"],"childField":["issueID"]},
                             "subquery":{"table":"comments"},"system":"client","hidden":false}}
               ]},
               "related":[{"correlation":{"parentField":["id"],"childField":["issueID"]},
                           "subquery":{"table":"comments","alias":"c"},"system":"client"}],
               "start":{"row":{"id":5},"exclusive":true},
               "limit":10,"orderBy":[["id","asc"]]}"#,
        );
        let json = ast_to_json(&ast);
        let round_tripped = ast_from_json(&json).unwrap();
        assert_eq!(ast, round_tripped);
    }

    #[test]
    fn ast_to_json_omits_absent_fields() {
        let ast = Ast::table("issues");
        let json = ast_to_json(&ast);
        assert_eq!(
            json,
            JsonValue::Object(vec![(
                "table".to_string(),
                JsonValue::String("issues".to_string())
            )])
        );
    }
}
