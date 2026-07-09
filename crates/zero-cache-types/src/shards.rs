//! Port of `zero-cache/src/types/shards.ts`.
//!
//! Identifies an app/shard and derives the Postgres schema names used by the
//! replication pipeline (upstream, CDC, and CVR schemas).
//!
//! The upstream TS file has no dedicated test; the tests here encode its
//! documented, self-evident behavior (schema naming and app-id validation).

use thiserror::Error;

/// An application id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppId {
    pub app_id: String,
}

/// An app id plus shard number. Port of `ShardID`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardId {
    pub app_id: String,
    pub shard_num: i64,
}

/// A shard plus its publications. Port of `ShardConfig`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardConfig {
    pub app_id: String,
    pub shard_num: i64,
    pub publications: Vec<String>,
}

/// Message used when an app id contains disallowed characters.
pub const INVALID_APP_ID_MESSAGE: &str =
    "The App ID may only consist of lower-case letters, numbers, and the underscore character";

/// Error returned when a [`ShardId`] fails validation.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct ShardError(pub &'static str);

/// Whether `app_id` matches `^[a-z0-9_]+$`. Port of `ALLOWED_APP_ID_CHARACTERS`.
fn allowed_app_id(app_id: &str) -> bool {
    !app_id.is_empty()
        && app_id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// Validates a [`ShardId`]. Port of `check`.
pub fn check(shard: &ShardId) -> Result<(&str, i64), ShardError> {
    if !allowed_app_id(&shard.app_id) {
        return Err(ShardError(INVALID_APP_ID_MESSAGE));
    }
    Ok((&shard.app_id, shard.shard_num))
}

/// The app schema (the app id itself). Port of `appSchema`.
pub fn app_schema(app: &AppId) -> Result<String, ShardError> {
    check(&ShardId {
        app_id: app.app_id.clone(),
        shard_num: 0,
    })?;
    Ok(app.app_id.clone())
}

/// The upstream schema: `{appID}_{shardNum}`. Port of `upstreamSchema`.
pub fn upstream_schema(shard: &ShardId) -> Result<String, ShardError> {
    let (app_id, shard_num) = check(shard)?;
    Ok(format!("{app_id}_{shard_num}"))
}

/// The CDC schema: `{appID}_{shardNum}/cdc`. Port of `cdcSchema`.
pub fn cdc_schema(shard: &ShardId) -> Result<String, ShardError> {
    let (app_id, shard_num) = check(shard)?;
    Ok(format!("{app_id}_{shard_num}/cdc"))
}

/// The CVR schema: `{appID}_{shardNum}/cvr`. Port of `cvrSchema`.
pub fn cvr_schema(shard: &ShardId) -> Result<String, ShardError> {
    let (app_id, shard_num) = check(shard)?;
    Ok(format!("{app_id}_{shard_num}/cvr"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(app_id: &str, shard_num: i64) -> ShardId {
        ShardId {
            app_id: app_id.to_string(),
            shard_num,
        }
    }

    #[test]
    fn schema_names() {
        let s = shard("zero", 0);
        assert_eq!(upstream_schema(&s).unwrap(), "zero_0");
        assert_eq!(cdc_schema(&s).unwrap(), "zero_0/cdc");
        assert_eq!(cvr_schema(&s).unwrap(), "zero_0/cvr");

        let s = shard("my_app", 3);
        assert_eq!(upstream_schema(&s).unwrap(), "my_app_3");
        assert_eq!(cdc_schema(&s).unwrap(), "my_app_3/cdc");
        assert_eq!(cvr_schema(&s).unwrap(), "my_app_3/cvr");
    }

    #[test]
    fn app_schema_returns_app_id() {
        assert_eq!(
            app_schema(&AppId {
                app_id: "zero".into()
            })
            .unwrap(),
            "zero"
        );
    }

    #[test]
    fn rejects_invalid_app_ids() {
        for bad in ["", "Zero", "my-app", "app.name", "with space", "UPPER"] {
            assert_eq!(
                upstream_schema(&shard(bad, 0)),
                Err(ShardError(INVALID_APP_ID_MESSAGE)),
                "{bad} should be rejected"
            );
        }
        for good in ["zero", "my_app", "app123", "_underscore", "a"] {
            assert!(check(&shard(good, 0)).is_ok(), "{good} should be allowed");
        }
    }
}
