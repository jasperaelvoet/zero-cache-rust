//! Port of five pure functions from `client-handler.ts`, alongside
//! `client_handler_poke.rs`: `makeRowPatch` (the internal `RowPatch` ->
//! wire `RowPatchOp` mapping `addPatch` uses), `ensureSafeJSON` (the
//! bigint-to-number safety conversion applied to a row's contents before
//! it's sent to a client), `#updateLMIDs` (the `zeroClientsTable` row ->
//! last-mutation-id change extraction), and BOTH the `zeroMutationsTable`
//! `'del'` branch's mutation-id parsing AND the `'put'` branch's
//! `mutationRowSchema`/`mutationResultSchema` row parsing (closing the
//! last named gap in `addPatch`'s branches).

use zero_cache_protocol::mutation_id::MutationId;
use zero_cache_protocol::mutation_result::{
    MutationAppError, MutationError, MutationOk, MutationResponse, MutationResult,
    MutationZeroError, ZeroErrorKind,
};
use zero_cache_protocol::row_patch::{
    PrimaryKeyValueRecord, Row as WireRow, RowDelOp, RowPatchOp, RowPutOp,
};
use zero_cache_shared::bigint_json::JsonValue;

use crate::client_patch::ClientRowPatch;

/// Port of `makeRowPatch`: converts the internal `RowPatch` (client-handler.ts,
/// ported here as [`ClientRowPatch`]) into the wire `RowPatchOp` a poke part
/// actually carries. A pure re-shaping — no `ensureSafeJSON`/schema
/// validation applied here (see [`ensure_safe_json`] for that, which a
/// caller runs on `contents` before calling this, matching upstream's
/// `v.parse(ensureSafeJSON(patch.contents), rowSchema)` composition).
pub fn make_row_patch(patch: &ClientRowPatch) -> RowPatchOp {
    match patch {
        ClientRowPatch::Put(p) => RowPatchOp::Put(RowPutOp {
            table_name: p.id.table.clone(),
            value: p.contents.clone(),
        }),
        ClientRowPatch::Delete(p) => {
            let id: PrimaryKeyValueRecord = p.id.row_key.clone().into_iter().collect();
            RowPatchOp::Del(RowDelOp {
                table_name: p.id.table.clone(),
                id,
            })
        }
    }
}

/// The safe integer range JS's `Number` can represent exactly. Port of
/// `Number.MIN_SAFE_INTEGER`/`Number.MAX_SAFE_INTEGER`.
const MIN_SAFE_INTEGER: i64 = -(2i64.pow(53)) + 1;
const MAX_SAFE_INTEGER: i64 = 2i64.pow(53) - 1;

/// Error from [`ensure_safe_json`]: a `BigInt` column value exceeds JS's
/// safe integer range and cannot be losslessly sent as a wire `Number`.
/// Port of `ensureSafeJSON`'s thrown `Error`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Value of \"{0}\" exceeds safe Number range ({1})")]
pub struct UnsafeIntegerError(pub String, pub String);

/// Port of `ensureSafeJSON`: `INT8`/`BIGINT` columns come back from the
/// query engine as [`JsonValue::BigInt`] (this port's `bigint` equivalent —
/// see `bigint_json`'s module doc); this converts any that fall within JS's
/// safe integer range to a plain [`JsonValue::Number`] (so the protocol can
/// carry values beyond a 32-bit range without a client-side bigint
/// dependency), and errors on any that don't. Non-`BigInt` values pass
/// through unchanged (upstream's `else if typeof v === 'object':
/// assertJSONValue(v)` is a pure validation upstream performs on plain JS
/// objects that doesn't apply here — this port's [`JsonValue`] is already
/// a closed, valid shape by construction).
pub fn ensure_safe_json(row: &WireRow) -> Result<WireRow, UnsafeIntegerError> {
    row.iter()
        .map(|(k, v)| match v {
            JsonValue::BigInt(n) => {
                let as_i64 = i64::try_from(n.clone()).ok();
                match as_i64 {
                    Some(i) if (MIN_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&i) => {
                        Ok((k.clone(), JsonValue::Number(i as f64)))
                    }
                    _ => Err(UnsafeIntegerError(k.clone(), n.to_string())),
                }
            }
            other => Ok((k.clone(), other.clone())),
        })
        .collect()
}

fn get<'a>(row: &'a WireRow, col: &str) -> Option<&'a JsonValue> {
    row.iter().find(|(k, _)| k == col).map(|(_, v)| v)
}

fn get_string(row: &WireRow, col: &str) -> Option<String> {
    match get(row, col) {
        Some(JsonValue::String(s)) => Some(s.clone()),
        _ => None,
    }
}

fn get_number(row: &WireRow, col: &str) -> Option<f64> {
    match get(row, col) {
        Some(JsonValue::Number(n)) => Some(*n),
        _ => None,
    }
}

/// Outcome of [`update_lmids`]: either a real last-mutation-id change for
/// one client, or a reason it was ignored (matching upstream's `#updateLMIDs`
/// — a wrong-`clientGroupID` row is logged and ignored, not an error).
#[derive(Debug, Clone, PartialEq)]
pub enum LmidUpdate {
    Change {
        client_id: String,
        last_mutation_id: f64,
    },
    IgnoredWrongClientGroup {
        received_client_group_id: String,
    },
}

/// Error parsing a `zeroClientsTable` row's expected `clientGroupID`/
/// `clientID`/`lastMutationID` fields (port of `lmidRowSchema`'s
/// validation — this crate has no schema-validation library, so this is a
/// direct field-presence/type check instead).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid clients row: missing or malformed clientGroupID/clientID/lastMutationID")]
pub struct LmidRowError;

/// Port of `#updateLMIDs`'s `'put'` branch (the `'constrain'`/`'del'` ops
/// are a documented no-op upstream — a caller simply doesn't call this for
/// those). `row` is the ALREADY-`ensure_safe_json`'d row contents, matching
/// upstream's `ensureSafeJSON(patch.contents)` call before parsing.
pub fn update_lmids(our_client_group_id: &str, row: &WireRow) -> Result<LmidUpdate, LmidRowError> {
    let client_group_id = get_string(row, "clientGroupID").ok_or(LmidRowError)?;
    let client_id = get_string(row, "clientID").ok_or(LmidRowError)?;
    let last_mutation_id = get_number(row, "lastMutationID").ok_or(LmidRowError)?;

    if client_group_id != our_client_group_id {
        return Ok(LmidUpdate::IgnoredWrongClientGroup {
            received_client_group_id: client_group_id,
        });
    }
    Ok(LmidUpdate::Change {
        client_id,
        last_mutation_id,
    })
}

/// Error from [`parse_mutation_del_id`] — port of the two `assert`s in
/// `addPatch`'s `zeroMutationsTable`/`'del'` branch.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MutationDelIdError {
    #[error("client id must be a string")]
    ClientIdNotAString,
    #[error("mutation id must be a finite number")]
    MutationIdNotAFiniteNonNegativeNumber,
}

/// Port of the `'del'` branch's id extraction from `patch.id.rowKey`:
/// `const {clientID, mutationID} = patch.id.rowKey; assert(typeof
/// clientID === 'string', ...); const id = Number(mutationID); assert(
/// !Number.isNaN(id) && Number.isFinite(id) && id >= 0, ...)`.
pub fn parse_mutation_del_id(
    row_key: &std::collections::BTreeMap<String, JsonValue>,
) -> Result<(String, f64), MutationDelIdError> {
    let client_id = match row_key.get("clientID") {
        Some(JsonValue::String(s)) => s.clone(),
        _ => return Err(MutationDelIdError::ClientIdNotAString),
    };
    let mutation_id = match row_key.get("mutationID") {
        Some(JsonValue::Number(n)) if n.is_finite() && *n >= 0.0 => *n,
        Some(JsonValue::String(s)) => match s.parse::<f64>() {
            Ok(n) if n.is_finite() && n >= 0.0 => n,
            _ => return Err(MutationDelIdError::MutationIdNotAFiniteNonNegativeNumber),
        },
        _ => return Err(MutationDelIdError::MutationIdNotAFiniteNonNegativeNumber),
    };
    Ok((client_id, mutation_id))
}

/// Error parsing a `zeroMutationsTable` row's `'put'` contents. Port of
/// `mutationRowSchema`/`mutationResultSchema`'s validation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MutationRowError {
    #[error("mutations row missing or malformed clientID/mutationID")]
    MissingIdFields,
    #[error("mutations row \"result\" field must be a JSON object")]
    ResultNotAnObject,
    #[error("mutations row \"result.error\" must be a recognized error kind, got {0:?}")]
    UnrecognizedErrorKind(String),
}

/// Port of `mutationResultSchema`'s discriminated union: `{data?}` (ok),
/// `{error: 'app', message?, details?}`, or `{error: 'oooMutation' |
/// 'alreadyProcessed', details?}` — discriminated on the presence/value of
/// the `error` field, matching upstream's `v.union` resolution order.
pub fn parse_mutation_result(value: &JsonValue) -> Result<MutationResult, MutationRowError> {
    let JsonValue::Object(fields) = value else {
        return Err(MutationRowError::ResultNotAnObject);
    };
    let field = |name: &str| fields.iter().find(|(k, _)| k == name).map(|(_, v)| v);

    match field("error") {
        None => Ok(MutationResult::Ok(MutationOk {
            data: field("data").cloned(),
        })),
        Some(JsonValue::String(kind)) if kind == "app" => Ok(MutationResult::Error(
            MutationError::App(MutationAppError {
                message: match field("message") {
                    Some(JsonValue::String(s)) => Some(s.clone()),
                    _ => None,
                },
                details: field("details").cloned(),
            }),
        )),
        Some(JsonValue::String(kind)) if kind == "oooMutation" => Ok(MutationResult::Error(
            MutationError::Zero(MutationZeroError {
                error: ZeroErrorKind::OooMutation,
                details: field("details").cloned(),
            }),
        )),
        Some(JsonValue::String(kind)) if kind == "alreadyProcessed" => Ok(MutationResult::Error(
            MutationError::Zero(MutationZeroError {
                error: ZeroErrorKind::AlreadyProcessed,
                details: field("details").cloned(),
            }),
        )),
        Some(JsonValue::String(kind)) => Err(MutationRowError::UnrecognizedErrorKind(kind.clone())),
        Some(_) => Err(MutationRowError::UnrecognizedErrorKind(
            "<non-string>".to_string(),
        )),
    }
}

/// Port of the `zeroMutationsTable` `'put'` branch's row parsing: `const
/// row = v.parse(ensureSafeJSON(patch.contents), mutationRowSchema,
/// 'passthrough'); patches.push({op: 'put', mutation: {id: {clientID:
/// row.clientID, id: row.mutationID}, result: row.result}})`. `contents`
/// is the ALREADY-`ensure_safe_json`'d row, matching upstream's call
/// order.
pub fn parse_mutation_put(contents: &WireRow) -> Result<MutationResponse, MutationRowError> {
    let client_id = get_string(contents, "clientID").ok_or(MutationRowError::MissingIdFields)?;
    let mutation_id =
        get_number(contents, "mutationID").ok_or(MutationRowError::MissingIdFields)?;
    let result_value = get(contents, "result").ok_or(MutationRowError::ResultNotAnObject)?;
    let result = parse_mutation_result(result_value)?;
    Ok(MutationResponse {
        id: MutationId {
            id: mutation_id,
            client_id,
        },
        result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_patch::{ClientDeleteRowPatch, ClientPutRowPatch};
    use crate::cvr_types::RowId;
    use num_bigint::BigInt;
    use std::collections::BTreeMap;

    fn row_id(table: &str, key: &str) -> RowId {
        RowId {
            schema: "public".into(),
            table: table.into(),
            row_key: BTreeMap::from([("id".to_string(), JsonValue::String(key.into()))]),
        }
    }

    #[test]
    fn make_row_patch_put_carries_table_name_and_contents() {
        let patch = ClientRowPatch::Put(ClientPutRowPatch {
            id: row_id("issues", "1"),
            contents: vec![("title".into(), JsonValue::String("bug".into()))],
        });
        let wire = make_row_patch(&patch);
        match wire {
            RowPatchOp::Put(p) => {
                assert_eq!(p.table_name, "issues");
                assert_eq!(
                    p.value,
                    vec![("title".to_string(), JsonValue::String("bug".into()))]
                );
            }
            other => panic!("expected Put, got {other:?}"),
        }
    }

    #[test]
    fn make_row_patch_del_carries_table_name_and_primary_key() {
        let patch = ClientRowPatch::Delete(ClientDeleteRowPatch {
            id: row_id("issues", "42"),
        });
        let wire = make_row_patch(&patch);
        match wire {
            RowPatchOp::Del(p) => {
                assert_eq!(p.table_name, "issues");
                assert_eq!(p.id.get("id"), Some(&JsonValue::String("42".into())));
            }
            other => panic!("expected Del, got {other:?}"),
        }
    }

    #[test]
    fn ensure_safe_json_converts_in_range_bigint_to_number() {
        let row = vec![("count".to_string(), JsonValue::BigInt(BigInt::from(42)))];
        let result = ensure_safe_json(&row).unwrap();
        assert_eq!(result, vec![("count".to_string(), JsonValue::Number(42.0))]);
    }

    #[test]
    fn ensure_safe_json_passes_through_non_bigint_values() {
        let row = vec![
            ("name".to_string(), JsonValue::String("hi".into())),
            ("active".to_string(), JsonValue::Bool(true)),
            ("n".to_string(), JsonValue::Null),
        ];
        let result = ensure_safe_json(&row).unwrap();
        assert_eq!(result, row);
    }

    #[test]
    fn ensure_safe_json_errors_on_bigint_exceeding_safe_range() {
        let too_big = BigInt::from(MAX_SAFE_INTEGER) + BigInt::from(1);
        let row = vec![("huge".to_string(), JsonValue::BigInt(too_big))];
        let err = ensure_safe_json(&row).unwrap_err();
        assert_eq!(err.0, "huge");
    }

    #[test]
    fn ensure_safe_json_errors_on_bigint_below_min_safe_range() {
        let too_small = BigInt::from(MIN_SAFE_INTEGER) - BigInt::from(1);
        let row = vec![("huge_neg".to_string(), JsonValue::BigInt(too_small))];
        assert!(ensure_safe_json(&row).is_err());
    }

    #[test]
    fn ensure_safe_json_accepts_the_exact_boundary_values() {
        let row = vec![
            (
                "max".to_string(),
                JsonValue::BigInt(BigInt::from(MAX_SAFE_INTEGER)),
            ),
            (
                "min".to_string(),
                JsonValue::BigInt(BigInt::from(MIN_SAFE_INTEGER)),
            ),
        ];
        let result = ensure_safe_json(&row).unwrap();
        assert_eq!(
            result,
            vec![
                (
                    "max".to_string(),
                    JsonValue::Number(MAX_SAFE_INTEGER as f64)
                ),
                (
                    "min".to_string(),
                    JsonValue::Number(MIN_SAFE_INTEGER as f64)
                )
            ]
        );
    }

    fn lmid_row(client_group_id: &str, client_id: &str, last_mutation_id: f64) -> WireRow {
        vec![
            (
                "clientGroupID".to_string(),
                JsonValue::String(client_group_id.into()),
            ),
            ("clientID".to_string(), JsonValue::String(client_id.into())),
            (
                "lastMutationID".to_string(),
                JsonValue::Number(last_mutation_id),
            ),
        ]
    }

    #[test]
    fn update_lmids_returns_a_change_for_matching_client_group() {
        let row = lmid_row("cg1", "c1", 5.0);
        assert_eq!(
            update_lmids("cg1", &row).unwrap(),
            LmidUpdate::Change {
                client_id: "c1".into(),
                last_mutation_id: 5.0
            }
        );
    }

    #[test]
    fn update_lmids_ignores_a_row_for_a_different_client_group() {
        let row = lmid_row("cg-other", "c1", 5.0);
        assert_eq!(
            update_lmids("cg1", &row).unwrap(),
            LmidUpdate::IgnoredWrongClientGroup {
                received_client_group_id: "cg-other".into()
            }
        );
    }

    #[test]
    fn update_lmids_errors_on_missing_fields() {
        let row = vec![("clientGroupID".to_string(), JsonValue::String("cg1".into()))];
        assert_eq!(update_lmids("cg1", &row), Err(LmidRowError));
    }

    #[test]
    fn parse_mutation_del_id_accepts_valid_ids() {
        let row_key = std::collections::BTreeMap::from([
            ("clientID".to_string(), JsonValue::String("c1".into())),
            ("mutationID".to_string(), JsonValue::Number(3.0)),
        ]);
        assert_eq!(
            parse_mutation_del_id(&row_key).unwrap(),
            ("c1".to_string(), 3.0)
        );
    }

    #[test]
    fn parse_mutation_del_id_accepts_numeric_string_mutation_id() {
        let row_key = std::collections::BTreeMap::from([
            ("clientID".to_string(), JsonValue::String("c1".into())),
            ("mutationID".to_string(), JsonValue::String("7".into())),
        ]);
        assert_eq!(
            parse_mutation_del_id(&row_key).unwrap(),
            ("c1".to_string(), 7.0)
        );
    }

    #[test]
    fn parse_mutation_del_id_rejects_non_string_client_id() {
        let row_key = std::collections::BTreeMap::from([
            ("clientID".to_string(), JsonValue::Number(1.0)),
            ("mutationID".to_string(), JsonValue::Number(3.0)),
        ]);
        assert_eq!(
            parse_mutation_del_id(&row_key),
            Err(MutationDelIdError::ClientIdNotAString)
        );
    }

    #[test]
    fn parse_mutation_del_id_rejects_negative_mutation_id() {
        let row_key = std::collections::BTreeMap::from([
            ("clientID".to_string(), JsonValue::String("c1".into())),
            ("mutationID".to_string(), JsonValue::Number(-1.0)),
        ]);
        assert_eq!(
            parse_mutation_del_id(&row_key),
            Err(MutationDelIdError::MutationIdNotAFiniteNonNegativeNumber)
        );
    }

    #[test]
    fn parse_mutation_del_id_rejects_non_finite_mutation_id() {
        let row_key = std::collections::BTreeMap::from([
            ("clientID".to_string(), JsonValue::String("c1".into())),
            ("mutationID".to_string(), JsonValue::Number(f64::NAN)),
        ]);
        assert_eq!(
            parse_mutation_del_id(&row_key),
            Err(MutationDelIdError::MutationIdNotAFiniteNonNegativeNumber)
        );
    }

    fn obj(pairs: &[(&str, JsonValue)]) -> JsonValue {
        JsonValue::Object(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    #[test]
    fn parse_mutation_result_ok_with_data() {
        let value = obj(&[("data", JsonValue::String("hi".into()))]);
        assert_eq!(
            parse_mutation_result(&value).unwrap(),
            MutationResult::Ok(MutationOk {
                data: Some(JsonValue::String("hi".into()))
            })
        );
    }

    #[test]
    fn parse_mutation_result_ok_without_data() {
        let value = obj(&[]);
        assert_eq!(
            parse_mutation_result(&value).unwrap(),
            MutationResult::Ok(MutationOk { data: None })
        );
    }

    #[test]
    fn parse_mutation_result_app_error() {
        let value = obj(&[
            ("error", JsonValue::String("app".into())),
            ("message", JsonValue::String("oops".into())),
        ]);
        assert_eq!(
            parse_mutation_result(&value).unwrap(),
            MutationResult::Error(MutationError::App(MutationAppError {
                message: Some("oops".into()),
                details: None
            }))
        );
    }

    #[test]
    fn parse_mutation_result_zero_error_variants() {
        let ooo = obj(&[("error", JsonValue::String("oooMutation".into()))]);
        assert_eq!(
            parse_mutation_result(&ooo).unwrap(),
            MutationResult::Error(MutationError::Zero(MutationZeroError {
                error: ZeroErrorKind::OooMutation,
                details: None
            }))
        );

        let already = obj(&[("error", JsonValue::String("alreadyProcessed".into()))]);
        assert_eq!(
            parse_mutation_result(&already).unwrap(),
            MutationResult::Error(MutationError::Zero(MutationZeroError {
                error: ZeroErrorKind::AlreadyProcessed,
                details: None
            }))
        );
    }

    #[test]
    fn parse_mutation_result_rejects_unrecognized_error_kind() {
        let value = obj(&[("error", JsonValue::String("bogus".into()))]);
        assert_eq!(
            parse_mutation_result(&value),
            Err(MutationRowError::UnrecognizedErrorKind("bogus".to_string()))
        );
    }

    #[test]
    fn parse_mutation_result_rejects_non_object() {
        assert_eq!(
            parse_mutation_result(&JsonValue::Null),
            Err(MutationRowError::ResultNotAnObject)
        );
    }

    #[test]
    fn parse_mutation_put_builds_a_full_mutation_response() {
        let contents = vec![
            ("clientGroupID".to_string(), JsonValue::String("cg1".into())),
            ("clientID".to_string(), JsonValue::String("c1".into())),
            ("mutationID".to_string(), JsonValue::Number(7.0)),
            (
                "result".to_string(),
                obj(&[("data", JsonValue::Bool(true))]),
            ),
        ];
        let response = parse_mutation_put(&contents).unwrap();
        assert_eq!(
            response.id,
            MutationId {
                id: 7.0,
                client_id: "c1".into()
            }
        );
        assert_eq!(
            response.result,
            MutationResult::Ok(MutationOk {
                data: Some(JsonValue::Bool(true))
            })
        );
    }

    #[test]
    fn parse_mutation_put_errors_on_missing_id_fields() {
        let contents = vec![("result".to_string(), obj(&[]))];
        assert_eq!(
            parse_mutation_put(&contents),
            Err(MutationRowError::MissingIdFields)
        );
    }
}
