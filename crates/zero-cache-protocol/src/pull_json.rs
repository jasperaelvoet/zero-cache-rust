//! Wire decode (`pull` request) and encode (`pull` response) for
//! [`crate::pull`].

use zero_cache_shared::bigint_json::{stringify as json_value_stringify, JsonValue};

use crate::pull::{PullRequestBody, PullResponseBody};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("pull message JSON: {0}")]
pub struct PullJsonError(pub String);

type R<T> = Result<T, PullJsonError>;

fn err<T>(m: impl Into<String>) -> R<T> {
    Err(PullJsonError(m.into()))
}
fn field<'a>(o: &'a JsonValue, k: &str) -> Option<&'a JsonValue> {
    match o {
        JsonValue::Object(es) => es.iter().find(|(n, _)| n == k).map(|(_, v)| v),
        _ => None,
    }
}
fn req<'a>(o: &'a JsonValue, k: &str) -> R<&'a JsonValue> {
    field(o, k).ok_or_else(|| PullJsonError(format!("missing field {k:?}")))
}
fn as_str(v: &JsonValue) -> R<String> {
    match v {
        JsonValue::String(s) => Ok(s.clone()),
        other => err(format!("expected string, got {other:?}")),
    }
}
fn json_string(s: &str) -> String {
    json_value_stringify(&JsonValue::String(s.to_string()))
}

/// Parses a `pull` message's body (`pullRequestBodySchema`), not including the
/// outer `["pull", ...]` tag envelope.
pub fn pull_request_body_from_json(v: &JsonValue) -> R<PullRequestBody> {
    let cookie = match req(v, "cookie")? {
        JsonValue::Null => None,
        JsonValue::String(s) => Some(s.clone()),
        other => return err(format!("expected string or null cookie, got {other:?}")),
    };
    Ok(PullRequestBody {
        client_group_id: as_str(req(v, "clientGroupID")?)?,
        cookie,
        request_id: as_str(req(v, "requestID")?)?,
    })
}

/// Encodes a full `["pull", {"cookie": ..., "requestID": ...,
/// "lastMutationIDChanges": {...}}]` response frame.
pub fn pull_response_message_json(body: &PullResponseBody) -> String {
    let changes: Vec<String> = body
        .last_mutation_id_changes
        .iter()
        .map(|(client_id, lmid)| format!("{}:{lmid}", json_string(client_id)))
        .collect();
    format!(
        "[\"pull\",{{\"cookie\":{},\"requestID\":{},\"lastMutationIDChanges\":{{{}}}}}]",
        json_string(&body.cookie),
        json_string(&body.request_id),
        changes.join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use zero_cache_shared::bigint_json::parse;

    #[test]
    fn decodes_pull_request_with_version_cookie() {
        let body = pull_request_body_from_json(
            &parse(r#"{"clientGroupID":"cg1","cookie":"01","requestID":"r1"}"#).unwrap(),
        )
        .unwrap();
        assert_eq!(body.client_group_id, "cg1");
        assert_eq!(body.cookie.as_deref(), Some("01"));
        assert_eq!(body.request_id, "r1");
    }

    #[test]
    fn decodes_pull_request_with_null_cookie() {
        let body = pull_request_body_from_json(
            &parse(r#"{"clientGroupID":"cg1","cookie":null,"requestID":"r1"}"#).unwrap(),
        )
        .unwrap();
        assert_eq!(body.cookie, None);
    }

    #[test]
    fn rejects_missing_or_wrong_cookie_type() {
        assert!(pull_request_body_from_json(
            &parse(r#"{"clientGroupID":"cg1","requestID":"r1"}"#).unwrap()
        )
        .is_err());
        assert!(pull_request_body_from_json(
            &parse(r#"{"clientGroupID":"cg1","cookie":1,"requestID":"r1"}"#).unwrap()
        )
        .is_err());
    }

    #[test]
    fn encodes_pull_response() {
        let mut changes = BTreeMap::new();
        changes.insert("c1".to_string(), 3.0);
        changes.insert("c2".to_string(), 4.0);
        let json = pull_response_message_json(&PullResponseBody {
            cookie: "02".into(),
            request_id: "r1".into(),
            last_mutation_id_changes: changes,
        });
        assert_eq!(
            json,
            r#"["pull",{"cookie":"02","requestID":"r1","lastMutationIDChanges":{"c1":3,"c2":4}}]"#
        );
    }

    #[test]
    fn response_escapes_strings() {
        let json = pull_response_message_json(&PullResponseBody {
            cookie: "0\"2".into(),
            request_id: "r\n1".into(),
            last_mutation_id_changes: BTreeMap::new(),
        });
        assert_eq!(json, "[\"pull\",{\"cookie\":\"0\\\"2\",\"requestID\":\"r\\n1\",\"lastMutationIDChanges\":{}}]");
    }
}
