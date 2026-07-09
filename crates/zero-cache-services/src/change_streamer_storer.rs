//! Port of two pure string-manipulation functions from
//! `services/change-streamer/storer.ts` (1068 lines) — `extractChangeSubstring`
//! and `toDownstream`, found while sweeping `storer.ts` for a tractable
//! slice ahead of the file's actual live orchestration (`Storer`, a
//! `TransactionPool`-backed change-log writer/subscriber-catchup service).
//!
//! Scope: NOT ported — `Storer` itself (live Postgres transaction pool,
//! subscriber catchup, change-log persistence), `PurgeLock`/`PurgeLocker`
//! (live transaction-scoped locking around change-log purges). All need
//! live Postgres/transaction-pool infrastructure this port doesn't have.

/// Port of `WatermarkedChange` (a `[watermark, tag, changeJSON]` tuple).
pub type WatermarkedChange = (String, String, String);

/// Port of `ChangeLogEntry`.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeLogEntry {
    pub watermark: String,
    pub tag: String,
    pub change: String,
}

/// Port of `extractChangeSubstring`: extracts the stringified change
/// message from the stringified stream message (e.g. `["begin",<message-
/// json>,{"commitWatermark":"..."}]` -> `<message-json>`). This
/// optimization lets the caller stringify the full stream message exactly
/// once while storing only the inner change-message substring in the
/// change log (for backwards compatibility with the log's own format).
///
/// `begin`/`commit` messages have a trailing metadata object after the
/// change JSON, so the substring ends at the LAST top-level comma; every
/// other tag (`data`, or `tag` unset) has no trailing element, so it ends
/// at the closing `]`.
pub fn extract_change_substring(stream_message_json: &str, tag: Option<&str>) -> String {
    let start = stream_message_json.find(',').map_or(0, |i| i + 1);
    let end = match tag {
        Some("begin") | Some("commit") => stream_message_json
            .rfind(',')
            .unwrap_or(stream_message_json.len()),
        _ => stream_message_json
            .rfind(']')
            .unwrap_or(stream_message_json.len()),
    };
    if end <= start {
        return String::new();
    }
    stream_message_json[start..end].to_string()
}

/// Port of `toDownstream`: reconstructs a full stream message from a
/// change-log entry's `(watermark, tag, change)` — the inverse of the
/// substring extraction [`extract_change_substring`] performs, re-adding
/// each tag's trailing metadata.
pub fn to_downstream(entry: &ChangeLogEntry) -> WatermarkedChange {
    let ChangeLogEntry {
        watermark,
        tag,
        change,
    } = entry;
    let message = match tag.as_str() {
        "begin" => format!("[\"begin\",{change},{{\"commitWatermark\":\"{watermark}\"}}]"),
        "commit" => format!("[\"commit\",{change},{{\"watermark\":\"{watermark}\"}}]"),
        "rollback" => format!("[\"rollback\",{change}]"),
        _ => format!("[\"data\",{change}]"),
    };
    (watermark.clone(), tag.clone(), message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_change_substring_for_begin_stops_before_the_trailing_metadata() {
        let msg = r#"["begin",<message-json>,{"commitWatermark":"92fj2d0s"}]"#;
        assert_eq!(
            extract_change_substring(msg, Some("begin")),
            "<message-json>"
        );
    }

    #[test]
    fn extract_change_substring_for_commit_stops_before_the_trailing_metadata() {
        let msg = r#"["commit",<message-json>,{"watermark":"92fj2d0s"}]"#;
        assert_eq!(
            extract_change_substring(msg, Some("commit")),
            "<message-json>"
        );
    }

    #[test]
    fn extract_change_substring_for_data_stops_at_the_closing_bracket() {
        let msg = r#"["data",<message-json>]"#;
        assert_eq!(
            extract_change_substring(msg, Some("data")),
            "<message-json>"
        );
    }

    #[test]
    fn extract_change_substring_defaults_to_the_bracket_form_when_tag_is_none() {
        let msg = r#"["rollback",<message-json>]"#;
        assert_eq!(extract_change_substring(msg, None), "<message-json>");
    }

    #[test]
    fn to_downstream_reconstructs_begin_and_commit_with_their_metadata() {
        let begin = ChangeLogEntry {
            watermark: "w1".into(),
            tag: "begin".into(),
            change: "<m>".into(),
        };
        assert_eq!(
            to_downstream(&begin),
            (
                "w1".to_string(),
                "begin".to_string(),
                "[\"begin\",<m>,{\"commitWatermark\":\"w1\"}]".to_string()
            )
        );

        let commit = ChangeLogEntry {
            watermark: "w2".into(),
            tag: "commit".into(),
            change: "<m>".into(),
        };
        assert_eq!(
            to_downstream(&commit),
            (
                "w2".to_string(),
                "commit".to_string(),
                "[\"commit\",<m>,{\"watermark\":\"w2\"}]".to_string()
            )
        );
    }

    #[test]
    fn to_downstream_reconstructs_rollback_and_data_without_metadata() {
        let rollback = ChangeLogEntry {
            watermark: "w3".into(),
            tag: "rollback".into(),
            change: "<m>".into(),
        };
        assert_eq!(
            to_downstream(&rollback),
            (
                "w3".to_string(),
                "rollback".to_string(),
                "[\"rollback\",<m>]".to_string()
            )
        );

        let data = ChangeLogEntry {
            watermark: "w4".into(),
            tag: "data".into(),
            change: "<m>".into(),
        };
        assert_eq!(
            to_downstream(&data),
            (
                "w4".to_string(),
                "data".to_string(),
                "[\"data\",<m>]".to_string()
            )
        );
    }

    #[test]
    fn extract_then_reconstruct_round_trips() {
        let original = r#"["begin",{"a":1},{"commitWatermark":"w5"}]"#;
        let change = extract_change_substring(original, Some("begin"));
        let entry = ChangeLogEntry {
            watermark: "w5".into(),
            tag: "begin".into(),
            change,
        };
        let (_, _, reconstructed) = to_downstream(&entry);
        assert_eq!(reconstructed, original);
    }
}
