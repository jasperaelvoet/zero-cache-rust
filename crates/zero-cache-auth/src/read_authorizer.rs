//! Port of `zero-cache/src/auth/read-authorizer.ts`'s pure query transform:
//! inject row `select` permission rules into an AST before it is planned or
//! analyzed, then hash the transformed AST.
//!
//! After adding permission conditions, upstream binds static `authData`
//! parameters from the authenticated JWT.  The same binding is implemented
//! here for both read queries and write-policy conditions; an absent claim
//! becomes `null`, matching upstream's `resolveField` behavior.

use std::collections::BTreeMap;

use crate::policy::{
    AssetPermissions, PermissionsConfig, Policy, TablePermissionsEntry, UpdatePolicies,
};
use zero_cache_protocol::ast::{
    Ast, Condition, CorrelatedSubquery, LiteralValue, Parameter, ValuePosition,
};
use zero_cache_protocol::query_hash::hash_of_ast;
use zero_cache_shared::bigint_json::JsonValue;

/// Port of `TransformedAndHashed`.
#[derive(Debug, Clone, PartialEq)]
pub struct TransformedAndHashed {
    pub id: String,
    pub transformed_ast: Ast,
    pub transformation_hash: String,
}

/// Port of `transformAndHashQuery`, minus auth static-parameter binding (see
/// module doc). Internal queries bypass application read permissions.
pub fn transform_and_hash_query(
    id: impl Into<String>,
    query: &Ast,
    permission_rules: &PermissionsConfig,
    internal_query: bool,
) -> TransformedAndHashed {
    let transformed = if internal_query {
        query.clone()
    } else {
        transform_query(query, permission_rules)
    };
    let transformation_hash = hash_of_ast(&transformed);
    TransformedAndHashed {
        id: id.into(),
        transformed_ast: transformed,
        transformation_hash,
    }
}

/// Like [`transform_and_hash_query`], but resolves compiled static references
/// against JWT `authData` before hashing/planning the query.
pub fn transform_and_hash_query_with_auth_data(
    id: impl Into<String>,
    query: &Ast,
    permission_rules: &PermissionsConfig,
    auth_data: &JsonValue,
    internal_query: bool,
) -> TransformedAndHashed {
    let transformed = if internal_query {
        bind_static_parameters(query, auth_data)
    } else {
        bind_static_parameters(&transform_query(query, permission_rules), auth_data)
    };
    let transformation_hash = hash_of_ast(&transformed);
    TransformedAndHashed {
        id: id.into(),
        transformed_ast: transformed,
        transformation_hash,
    }
}

/// Returns a copy of compiled permissions with static parameters bound to this
/// connection's auth data.  Write authorization evaluates policies directly,
/// so it uses this rather than going through a query transform.
pub fn bind_permissions_auth_data(
    permissions: &PermissionsConfig,
    auth_data: &JsonValue,
) -> PermissionsConfig {
    PermissionsConfig {
        tables: permissions.tables.as_ref().map(|tables| {
            tables
                .iter()
                .map(|(name, entry)| (name.clone(), bind_table_entry(entry, auth_data)))
                .collect::<BTreeMap<_, _>>()
        }),
    }
}

fn bind_table_entry(entry: &TablePermissionsEntry, auth_data: &JsonValue) -> TablePermissionsEntry {
    TablePermissionsEntry {
        row: entry
            .row
            .as_ref()
            .map(|asset| bind_asset_permissions(asset, auth_data)),
        cell: entry.cell.as_ref().map(|cells| {
            cells
                .iter()
                .map(|(name, asset)| (name.clone(), bind_asset_permissions(asset, auth_data)))
                .collect()
        }),
    }
}

fn bind_asset_permissions(asset: &AssetPermissions, auth_data: &JsonValue) -> AssetPermissions {
    AssetPermissions {
        select: bind_optional_policy(asset.select.as_ref(), auth_data),
        insert: bind_optional_policy(asset.insert.as_ref(), auth_data),
        update: UpdatePolicies {
            pre_mutation: bind_optional_policy(asset.update.pre_mutation.as_ref(), auth_data),
            post_mutation: bind_optional_policy(asset.update.post_mutation.as_ref(), auth_data),
        },
        delete: bind_optional_policy(asset.delete.as_ref(), auth_data),
    }
}

fn bind_optional_policy(policy: Option<&Policy>, auth_data: &JsonValue) -> Option<Policy> {
    policy.map(|policy| {
        policy
            .iter()
            .map(|condition| bind_condition(condition, auth_data))
            .collect()
    })
}

/// Binds static references in a query AST.  `authData` fields support both a
/// single field and a nested field path (`['properties', 'role']`).  The only
/// other static anchor supported by the wire grammar is `preMutationRow`; it
/// has no value in read queries, so it binds to `null` just as upstream does.
pub fn bind_static_parameters(ast: &Ast, auth_data: &JsonValue) -> Ast {
    let mut bound = ast.clone();
    bound.where_ = ast
        .where_
        .as_ref()
        .map(|condition| bind_condition(condition, auth_data));
    bound.related = ast.related.as_ref().map(|related| {
        related
            .iter()
            .map(|relation| CorrelatedSubquery {
                correlation: relation.correlation.clone(),
                subquery: Box::new(bind_static_parameters(&relation.subquery, auth_data)),
                system: relation.system,
                hidden: relation.hidden,
            })
            .collect()
    });
    bound
}

fn bind_condition(condition: &Condition, auth_data: &JsonValue) -> Condition {
    match condition {
        Condition::Simple { op, left, right } => Condition::Simple {
            op: *op,
            left: bind_value_position(left, auth_data),
            right: bind_value_position(right, auth_data),
        },
        Condition::And { conditions } => Condition::And {
            conditions: conditions
                .iter()
                .map(|condition| bind_condition(condition, auth_data))
                .collect(),
        },
        Condition::Or { conditions } => Condition::Or {
            conditions: conditions
                .iter()
                .map(|condition| bind_condition(condition, auth_data))
                .collect(),
        },
        Condition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
            plan_id,
        } => Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: related.correlation.clone(),
                subquery: Box::new(bind_static_parameters(&related.subquery, auth_data)),
                system: related.system,
                hidden: related.hidden,
            },
            op: *op,
            flip: *flip,
            scalar: *scalar,
            plan_id: *plan_id,
        },
    }
}

fn bind_value_position(value: &ValuePosition, auth_data: &JsonValue) -> ValuePosition {
    match value {
        ValuePosition::Parameter(parameter) => {
            ValuePosition::Literal(literal_from_json(resolve_parameter(parameter, auth_data)))
        }
        _ => value.clone(),
    }
}

fn literal_from_json(value: JsonValue) -> LiteralValue {
    match value {
        JsonValue::Null => LiteralValue::Null,
        JsonValue::Bool(value) => LiteralValue::Bool(value),
        JsonValue::Number(value) => LiteralValue::Number(value),
        JsonValue::BigInt(value) => value
            .to_string()
            .parse::<f64>()
            .map(LiteralValue::Number)
            .unwrap_or(LiteralValue::Null),
        JsonValue::String(value) => LiteralValue::String(value),
        JsonValue::Array(values) => {
            let mut literal_values = Vec::with_capacity(values.len());
            for value in values {
                let value = literal_from_json(value);
                if matches!(value, LiteralValue::Array(_)) {
                    return LiteralValue::Null;
                }
                literal_values.push(value);
            }
            LiteralValue::Array(literal_values)
        }
        // A static parameter cannot be an object in the Zero condition
        // grammar (literal references are scalar/scalar-array only).
        JsonValue::Object(_) => LiteralValue::Null,
    }
}

fn object_field<'a>(value: &'a JsonValue, name: &str) -> Option<&'a JsonValue> {
    match value {
        JsonValue::Object(fields) => fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value),
        _ => None,
    }
}

fn resolve_parameter(parameter: &Parameter, auth_data: &JsonValue) -> JsonValue {
    if !matches!(&parameter.raw, JsonValue::Object(_)) {
        return JsonValue::Null;
    }
    let anchor = object_field(&parameter.raw, "anchor");
    if !matches!(anchor, Some(JsonValue::String(anchor)) if anchor == "authData") {
        return JsonValue::Null;
    }
    let Some(field) = object_field(&parameter.raw, "field") else {
        return JsonValue::Null;
    };
    let path: Vec<&str> = match field {
        JsonValue::String(field) => vec![field],
        JsonValue::Array(parts) => {
            let mut path = Vec::with_capacity(parts.len());
            for part in parts {
                let JsonValue::String(part) = part else {
                    return JsonValue::Null;
                };
                path.push(part.as_str());
            }
            path
        }
        _ => return JsonValue::Null,
    };
    let mut value = auth_data;
    for part in path {
        let Some(next) = object_field(value, part) else {
            return JsonValue::Null;
        };
        value = next;
    }
    value.clone()
}

/// For a given AST, apply row-select read-auth rules recursively.
pub fn transform_query(query: &Ast, permission_rules: &PermissionsConfig) -> Ast {
    transform_query_internal(query, permission_rules)
}

fn transform_query_internal(query: &Ast, permission_rules: &PermissionsConfig) -> Ast {
    let row_select_rules = row_select_rules(permission_rules, &query.table);

    let updated_where = add_rules_to_where(
        query
            .where_
            .as_ref()
            .map(|where_| transform_condition(where_, permission_rules)),
        row_select_rules,
    );

    let mut transformed = query.clone();
    transformed.where_ = Some(simplify_condition(updated_where));
    transformed.related = query.related.as_ref().map(|related| {
        related
            .iter()
            .map(|sq| CorrelatedSubquery {
                correlation: sq.correlation.clone(),
                subquery: Box::new(transform_query_internal(&sq.subquery, permission_rules)),
                system: sq.system,
                hidden: sq.hidden,
            })
            .collect()
    });
    transformed
}

fn row_select_rules<'a>(
    permission_rules: &'a PermissionsConfig,
    table: &str,
) -> Option<&'a Policy> {
    permission_rules
        .tables
        .as_ref()
        .and_then(|tables| tables.get(table))
        .and_then(|entry| entry.row.as_ref())
        .and_then(|row| row.select.as_ref())
        .filter(|rules| !rules.is_empty())
}

fn add_rules_to_where(where_: Option<Condition>, row_select_rules: Option<&Policy>) -> Condition {
    let mut conditions = Vec::new();
    if let Some(where_) = where_ {
        conditions.push(where_);
    }
    conditions.push(Condition::Or {
        conditions: row_select_rules.cloned().unwrap_or_default(),
    });
    Condition::And { conditions }
}

// We must augment correlated subqueries in WHERE position too; otherwise a
// client could use `whereExists` as an oracle for rows they cannot read.
fn transform_condition(cond: &Condition, permission_rules: &PermissionsConfig) -> Condition {
    match cond {
        Condition::Simple { .. } => cond.clone(),
        Condition::And { conditions } => Condition::And {
            conditions: conditions
                .iter()
                .map(|c| transform_condition(c, permission_rules))
                .collect(),
        },
        Condition::Or { conditions } => Condition::Or {
            conditions: conditions
                .iter()
                .map(|c| transform_condition(c, permission_rules))
                .collect(),
        },
        Condition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
            plan_id,
        } => Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: related.correlation.clone(),
                subquery: Box::new(transform_query_internal(
                    &related.subquery,
                    permission_rules,
                )),
                system: related.system,
                hidden: related.hidden,
            },
            op: *op,
            flip: *flip,
            scalar: *scalar,
            plan_id: *plan_id,
        },
    }
}

fn simplify_condition(cond: Condition) -> Condition {
    match cond {
        Condition::Simple { .. } | Condition::CorrelatedSubquery { .. } => cond,
        Condition::And { conditions } => simplify_compound(true, conditions),
        Condition::Or { conditions } => simplify_compound(false, conditions),
    }
}

fn simplify_compound(is_and: bool, conditions: Vec<Condition>) -> Condition {
    if conditions.len() == 1 {
        return simplify_condition(conditions.into_iter().next().unwrap());
    }

    let mut flattened = Vec::new();
    for condition in conditions.into_iter().map(simplify_condition) {
        match (is_and, condition) {
            (true, Condition::And { conditions }) | (false, Condition::Or { conditions }) => {
                flattened.extend(conditions);
            }
            (_, condition) => flattened.push(condition),
        }
    }

    if is_and && flattened.iter().any(is_always_false) {
        return false_condition();
    }
    if !is_and && flattened.iter().any(is_always_true) {
        return true_condition();
    }

    if is_and {
        Condition::And {
            conditions: flattened,
        }
    } else {
        Condition::Or {
            conditions: flattened,
        }
    }
}

fn is_always_true(condition: &Condition) -> bool {
    matches!(condition, Condition::And { conditions } if conditions.is_empty())
}

fn is_always_false(condition: &Condition) -> bool {
    matches!(condition, Condition::Or { conditions } if conditions.is_empty())
}

fn true_condition() -> Condition {
    Condition::And { conditions: vec![] }
}

fn false_condition() -> Condition {
    Condition::Or { conditions: vec![] }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{AssetPermissions, TablePermissionsEntry};
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::{
        ColumnReference, Correlation, ExistsOp, LiteralValue, SimpleOperator, ValuePosition,
    };

    fn eq(column: &str, value: &str) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: column.to_string(),
            }),
            right: ValuePosition::Literal(LiteralValue::String(value.to_string())),
        }
    }

    fn permissions(entries: Vec<(&str, Policy)>) -> PermissionsConfig {
        let mut tables = BTreeMap::new();
        for (table, select) in entries {
            tables.insert(
                table.to_string(),
                TablePermissionsEntry {
                    row: Some(AssetPermissions {
                        select: Some(select),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }
        PermissionsConfig {
            tables: Some(tables),
        }
    }

    #[test]
    fn missing_select_policy_defaults_to_false_condition() {
        let transformed = transform_query(&Ast::table("issue"), &PermissionsConfig::default());

        assert_eq!(transformed.where_, Some(false_condition()));
    }

    #[test]
    fn select_policy_is_anded_with_existing_where() {
        let existing = eq("status", "open");
        let allowed = eq("owner", "alice");
        let transformed = transform_query(
            &Ast {
                table: "issue".to_string(),
                where_: Some(existing.clone()),
                ..Default::default()
            },
            &permissions(vec![("issue", vec![allowed.clone()])]),
        );

        assert_eq!(
            transformed.where_,
            Some(Condition::And {
                conditions: vec![existing, allowed]
            })
        );
    }

    #[test]
    fn select_policy_survives_flattening_a_compound_and_where() {
        // A query whose where is ALREADY an `and` forces simplify to flatten
        // `And[And[a,b], Or[rule]]` -> `And[a,b,rule]`. The permission rule must
        // survive the flatten; a flattening bug that dropped it would leak rows.
        let a = eq("status", "open");
        let b = eq("priority", "high");
        let allowed = eq("owner", "alice");
        let transformed = transform_query(
            &Ast {
                table: "issue".to_string(),
                where_: Some(Condition::And {
                    conditions: vec![a.clone(), b.clone()],
                }),
                ..Default::default()
            },
            &permissions(vec![("issue", vec![allowed.clone()])]),
        );
        assert_eq!(
            transformed.where_,
            Some(Condition::And {
                conditions: vec![a, b, allowed]
            }),
            "the select rule must remain AND-ed in after flattening"
        );
    }

    #[test]
    fn recurses_into_related_queries() {
        let transformed = transform_query(
            &Ast {
                table: "issue".to_string(),
                related: Some(vec![CorrelatedSubquery {
                    correlation: Correlation {
                        parent_field: vec!["id".to_string()],
                        child_field: vec!["issueID".to_string()],
                    },
                    subquery: Box::new(Ast::table("comment")),
                    system: None,
                    hidden: None,
                }]),
                ..Default::default()
            },
            &permissions(vec![
                ("issue", vec![eq("owner", "alice")]),
                ("comment", vec![eq("visibility", "public")]),
            ]),
        );

        let issue_where = transformed.where_.as_ref().unwrap();
        assert_eq!(issue_where, &eq("owner", "alice"));
        let comment = &transformed.related.as_ref().unwrap()[0].subquery;
        let comment_where = comment.where_.as_ref().unwrap();
        assert_eq!(comment_where, &eq("visibility", "public"));
    }

    #[test]
    fn recurses_into_correlated_subquery_conditions() {
        let where_ = Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".to_string()],
                    child_field: vec!["issueID".to_string()],
                },
                subquery: Box::new(Ast::table("reaction")),
                system: None,
                hidden: Some(true),
            },
            op: ExistsOp::Exists,
            flip: Some(false),
            scalar: Some(false),
            plan_id: Some(42),
        };
        let transformed = transform_query(
            &Ast {
                table: "issue".to_string(),
                where_: Some(where_),
                ..Default::default()
            },
            &permissions(vec![
                ("issue", vec![eq("owner", "alice")]),
                ("reaction", vec![eq("kind", "thumbs-up")]),
            ]),
        );

        let transformed_where = transformed.where_.as_ref().unwrap();
        let Condition::And { conditions } = transformed_where else {
            panic!("expected top-level AND");
        };
        let Condition::CorrelatedSubquery {
            related, plan_id, ..
        } = &conditions[0]
        else {
            panic!("expected correlated subquery condition");
        };
        assert_eq!(*plan_id, Some(42));
        assert_eq!(
            related.subquery.where_.as_ref().unwrap(),
            &eq("kind", "thumbs-up")
        );
    }

    #[test]
    fn internal_query_skips_permissions_and_hashes_original() {
        let query = Ast::table("issue");
        let result = transform_and_hash_query(
            "q1",
            &query,
            &permissions(vec![("issue", vec![eq("x", "y")])]),
            true,
        );

        assert_eq!(result.id, "q1");
        assert_eq!(result.transformed_ast, query);
        assert_eq!(result.transformation_hash, hash_of_ast(&query));
    }

    #[test]
    fn external_query_hashes_the_transformed_ast() {
        let query = Ast::table("issue");
        let result = transform_and_hash_query(
            "q1",
            &query,
            &permissions(vec![("issue", vec![eq("x", "y")])]),
            false,
        );

        assert_ne!(result.transformed_ast, query);
        assert_eq!(
            result.transformation_hash,
            hash_of_ast(&result.transformed_ast)
        );
    }

    #[test]
    fn auth_data_static_parameters_bind_in_read_rules_and_nested_paths() {
        let static_sub = ValuePosition::Parameter(Parameter {
            raw: zero_cache_shared::bigint_json::parse(
                r#"{"type":"static","anchor":"authData","field":["properties","role"]}"#,
            )
            .unwrap(),
        });
        let rule = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: "role".to_string(),
            }),
            right: static_sub,
        };
        let auth_data =
            zero_cache_shared::bigint_json::parse(r#"{"sub":"u1","properties":{"role":"admin"}}"#)
                .unwrap();
        let result = transform_and_hash_query_with_auth_data(
            "q1",
            &Ast::table("issue"),
            &permissions(vec![("issue", vec![rule])]),
            &auth_data,
            false,
        );
        let Condition::Simple { right, .. } = result.transformed_ast.where_.as_ref().unwrap()
        else {
            panic!("expected bound rule");
        };
        assert_eq!(
            right,
            &ValuePosition::Literal(LiteralValue::String("admin".into()))
        );
        assert_eq!(
            result.transformation_hash,
            hash_of_ast(&result.transformed_ast)
        );
    }

    #[test]
    fn missing_auth_claim_binds_to_null_in_permissions() {
        let static_sub = ValuePosition::Parameter(Parameter {
            raw: zero_cache_shared::bigint_json::parse(
                r#"{"type":"static","anchor":"authData","field":"sub"}"#,
            )
            .unwrap(),
        });
        let config = permissions(vec![(
            "issue",
            vec![Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "owner".to_string(),
                }),
                right: static_sub,
            }],
        )]);
        let bound = bind_permissions_auth_data(
            &config,
            &zero_cache_shared::bigint_json::JsonValue::Object(vec![]),
        );
        let tables = bound.tables.as_ref().unwrap();
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
        assert_eq!(right, &ValuePosition::Literal(LiteralValue::Null));
    }
}
