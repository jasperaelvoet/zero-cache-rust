//! Decoding for the JSON shape emitted by `zero-schema`'s compiled
//! `definePermissions` output.
//!
//! Deployments commonly provide this as the `permissions` member of
//! `ZERO_SCHEMA_JSON`; the permissions metadata table stores the inner object
//! directly.  Accept both forms so the server has one safe parser for either
//! source.  A malformed configured policy is an error rather than something we
//! silently treat as unrestricted access.

use std::collections::BTreeMap;

use thiserror::Error;
use zero_cache_protocol::ast::Condition;
use zero_cache_protocol::ast_json::{condition_from_json, AstJsonError};
use zero_cache_shared::bigint_json::{parse, JsonValue};

use crate::policy::{
    AssetPermissions, PermissionsConfig, Policy, TablePermissionsEntry, UpdatePolicies,
};

#[derive(Debug, Error)]
pub enum PermissionsParseError {
    #[error("invalid permissions JSON: {0}")]
    Json(String),
    #[error("invalid compiled permissions: {0}")]
    Shape(String),
    #[error(transparent)]
    Condition(#[from] AstJsonError),
}

type Result<T> = std::result::Result<T, PermissionsParseError>;

fn field<'a>(object: &'a [(String, JsonValue)], name: &str) -> Option<&'a JsonValue> {
    object
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
}

fn object<'a>(value: &'a JsonValue, context: &str) -> Result<&'a [(String, JsonValue)]> {
    match value {
        JsonValue::Object(fields) => Ok(fields),
        _ => Err(PermissionsParseError::Shape(format!(
            "{context} must be an object"
        ))),
    }
}

fn array<'a>(value: &'a JsonValue, context: &str) -> Result<&'a [JsonValue]> {
    match value {
        JsonValue::Array(values) => Ok(values),
        _ => Err(PermissionsParseError::Shape(format!(
            "{context} must be an array"
        ))),
    }
}

fn string<'a>(value: &'a JsonValue, context: &str) -> Result<&'a str> {
    match value {
        JsonValue::String(value) => Ok(value),
        _ => Err(PermissionsParseError::Shape(format!(
            "{context} must be a string"
        ))),
    }
}

/// The current protocol condition grammar represents boolean constants as
/// empty conjunction/disjunction.  `ANYONE_CAN` is occasionally serialized by
/// development tooling as `{type: 'literal', value: true}`; accept that
/// harmless shorthand as well as the canonical condition grammar.
fn compiled_condition_from_json(value: &JsonValue) -> Result<Condition> {
    if let JsonValue::Object(fields) = value {
        if matches!(field(fields, "type"), Some(JsonValue::String(kind)) if kind == "literal") {
            return match field(fields, "value") {
                Some(JsonValue::Bool(true)) => Ok(Condition::And { conditions: vec![] }),
                Some(JsonValue::Bool(false)) => Ok(Condition::Or { conditions: vec![] }),
                _ => Err(PermissionsParseError::Shape(
                    "literal policy condition must contain a boolean value".to_string(),
                )),
            };
        }
    }
    Ok(condition_from_json(value)?)
}

fn policy(value: &JsonValue, context: &str) -> Result<Policy> {
    array(value, context)?
        .iter()
        .enumerate()
        .map(|(index, rule)| {
            let rule = array(rule, &format!("{context}[{index}]"))?;
            if rule.len() != 2 {
                return Err(PermissionsParseError::Shape(format!(
                    "{context}[{index}] must be ['allow', condition]"
                )));
            }
            if string(&rule[0], &format!("{context}[{index}][0]"))? != "allow" {
                return Err(PermissionsParseError::Shape(format!(
                    "{context}[{index}][0] must be 'allow'"
                )));
            }
            compiled_condition_from_json(&rule[1])
        })
        .collect()
}

fn optional_policy(
    object: &[(String, JsonValue)],
    name: &str,
    context: &str,
) -> Result<Option<Policy>> {
    field(object, name)
        .map(|value| policy(value, &format!("{context}.{name}")))
        .transpose()
}

fn asset_permissions(value: &JsonValue, context: &str) -> Result<AssetPermissions> {
    let value = object(value, context)?;
    let update = match field(value, "update") {
        Some(update) => {
            let update = object(update, &format!("{context}.update"))?;
            UpdatePolicies {
                pre_mutation: optional_policy(update, "preMutation", &format!("{context}.update"))?,
                post_mutation: optional_policy(
                    update,
                    "postMutation",
                    &format!("{context}.update"),
                )?,
            }
        }
        None => UpdatePolicies::default(),
    };
    Ok(AssetPermissions {
        select: optional_policy(value, "select", context)?,
        insert: optional_policy(value, "insert", context)?,
        update,
        delete: optional_policy(value, "delete", context)?,
    })
}

fn table_permissions(value: &JsonValue) -> Result<TablePermissionsEntry> {
    let value = object(value, "permissions.tables.<table>")?;
    let row = field(value, "row")
        .map(|value| asset_permissions(value, "permissions.tables.<table>.row"))
        .transpose()?;
    let cell = match field(value, "cell") {
        Some(cells) => {
            let cells = object(cells, "permissions.tables.<table>.cell")?;
            let mut parsed = BTreeMap::new();
            for (column, permissions) in cells {
                parsed.insert(
                    column.clone(),
                    asset_permissions(
                        permissions,
                        &format!("permissions.tables.<table>.cell.{column}"),
                    )?,
                );
            }
            Some(parsed)
        }
        None => None,
    };
    Ok(TablePermissionsEntry { row, cell })
}

/// Parses a compiled permissions object or a full `ZERO_SCHEMA_JSON` document.
///
/// The full document must include a non-null `permissions` member.  Leaving
/// `ZERO_SCHEMA_JSON` unset preserves the server's legacy no-config behavior;
/// setting it to a document that forgot permissions must not accidentally turn
/// enforcement off.
pub fn parse_compiled_permissions(value: &JsonValue) -> Result<PermissionsConfig> {
    let root = object(value, "root")?;
    let permissions = match field(root, "permissions") {
        Some(JsonValue::Null) => {
            return Err(PermissionsParseError::Shape(
                "root.permissions must not be null".to_string(),
            ));
        }
        Some(value) => value,
        None if field(root, "schema").is_some() => {
            return Err(PermissionsParseError::Shape(
                "ZERO_SCHEMA_JSON is missing its permissions member".to_string(),
            ));
        }
        None => value,
    };
    let permissions = object(permissions, "permissions")?;
    let tables = match field(permissions, "tables") {
        None => None,
        Some(tables) => {
            let tables = object(tables, "permissions.tables")?;
            let mut parsed = BTreeMap::new();
            for (table, permissions) in tables {
                parsed.insert(table.clone(), table_permissions(permissions)?);
            }
            Some(parsed)
        }
    };
    Ok(PermissionsConfig { tables })
}

/// Parses [`parse_compiled_permissions`] directly from JSON text.
pub fn parse_compiled_permissions_json(input: &str) -> Result<PermissionsConfig> {
    let value = parse(input).map_err(|error| PermissionsParseError::Json(error.to_string()))?;
    parse_compiled_permissions(&value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::{Condition, ValuePosition};

    #[test]
    fn parses_full_zero_schema_json_and_anyone_can_shorthand() {
        let permissions = parse_compiled_permissions_json(
            r#"{
              "permissions":{"tables":{"issue":{"row":{"select":[["allow",{"type":"literal","value":true}]]}}}},
              "schema":{"version":1,"tables":{}}
            }"#,
        )
        .unwrap();
        let tables = permissions.tables.as_ref().unwrap();
        let select = tables["issue"]
            .row
            .as_ref()
            .unwrap()
            .select
            .clone()
            .unwrap();
        assert_eq!(select, vec![Condition::And { conditions: vec![] }]);
    }

    #[test]
    fn parses_static_auth_data_conditions() {
        let permissions = parse_compiled_permissions_json(
            r#"{"tables":{"issue":{"row":{"select":[["allow",{
              "type":"simple","op":"=",
              "left":{"type":"column","name":"owner"},
              "right":{"type":"static","anchor":"authData","field":"sub"}
            }]]}}}}"#,
        )
        .unwrap();
        let tables = permissions.tables.as_ref().unwrap();
        let rule = &tables["issue"]
            .row
            .as_ref()
            .unwrap()
            .select
            .as_ref()
            .unwrap()[0];
        let Condition::Simple { right, .. } = rule else {
            panic!("expected simple condition");
        };
        assert!(matches!(right, ValuePosition::Parameter(_)));
    }

    #[test]
    fn rejects_a_schema_document_without_permissions() {
        let error = parse_compiled_permissions_json(r#"{"schema":{"version":1}}"#).unwrap_err();
        assert!(error.to_string().contains("missing its permissions"));
    }

    #[test]
    fn rejects_non_allow_rules() {
        let error = parse_compiled_permissions_json(
            r#"{"tables":{"issue":{"row":{"select":[["deny",{"type":"and","conditions":[]}]]}}}}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must be 'allow'"));
    }
}
