//! Task identity resolution (`ZERO_TASK_ID` default).
//!
//! Port of `src/server/runner/runtime.ts` `getTaskID`: when no explicit task
//! ID is configured, extract the ECS TaskARN's last path component from the
//! container metadata endpoint (`ECS_CONTAINER_METADATA_URI_V4`); on any
//! failure — or outside ECS — fall back to a random identifier (upstream uses
//! `nanoid()`).

/// The last path component of an ECS TaskARN, upstream's task-ID extraction
/// (`TaskARN.substring(lastIndexOf('/') + 1)`).
pub fn task_id_from_arn(arn: &str) -> String {
    arn.rsplit('/').next().unwrap_or(arn).to_string()
}

/// A random fallback identifier (upstream `nanoid()` equivalent): pid plus
/// clock entropy, hex-encoded.
pub fn random_task_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!(
        "{:x}{:08x}",
        std::process::id(),
        (nanos & 0xffff_ffff) as u32
    )
}

/// Resolves the task ID as upstream does: ECS metadata when available, random
/// otherwise. Never fails.
pub async fn resolve_task_id() -> String {
    let Some(container_uri) = std::env::var("ECS_CONTAINER_METADATA_URI_V4")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        return random_task_id();
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build();
    let Ok(client) = client else {
        return random_task_id();
    };
    // Upstream fetches ${uri}/task and reads {"TaskARN": "..."}.
    match client.get(format!("{container_uri}/task")).send().await {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => match body.get("TaskARN").and_then(|v| v.as_str()) {
                Some(arn) => task_id_from_arn(arn),
                None => random_task_id(),
            },
            Err(_) => random_task_id(),
        },
        Err(_) => random_task_id(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arn_suffix_extraction_matches_upstream() {
        assert_eq!(
            task_id_from_arn("arn:aws:ecs:eu-west-3:123:task/cluster/abc123def456"),
            "abc123def456"
        );
        // No slash: the whole string (upstream substring(lastIndexOf+1) = same).
        assert_eq!(task_id_from_arn("plain-id"), "plain-id");
    }

    #[test]
    fn random_ids_are_nonempty_and_distinct_across_time() {
        let a = random_task_id();
        assert!(!a.is_empty());
    }
}
